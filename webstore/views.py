import os
import logging 

from flask import Blueprint, current_app
from flask import request, url_for, g, send_file

from sqlalchemy.sql.expression import asc, desc
from sqlalchemy.sql.expression import select
from sqlalchemy import func
from sqlalchemy.exc import OperationalError, DatabaseError, \
        StatementError

from webstore.formats import render_table, render_message
from webstore.formats import read_request, response_format
from webstore.formats import SQLITE
from webstore.helpers import WebstoreException
from webstore.helpers import crossdomain, result_proxy_iterator
from webstore.validation import NamingException
from webstore.security import require, has
from webstore.database import SQLiteDatabaseHandlerFactory, UserNotFound

log = logging.getLogger(__name__)
store = Blueprint('webstore', __name__)
db_factory = SQLiteDatabaseHandlerFactory(current_app)

@store.before_app_request
def jsonp_callback_register():
    # This is a slight hack to not make JSON-P callback names 
    # end up in the query string, we'll keep it around on the 
    # request global and then read it in the table generator.
    g.callback = request.args.get('_callback')

def _get_table(user, database, table, format):
    """ Locate a named table or raise a 404. """
    try:
        db = db_factory.create(user, database)
    except NamingException, ne:
        raise WebstoreException('Invalid DB name: %s' % ne.field,
                format, state='error', code=400)
    if not table in db:
        raise WebstoreException('No such table: %s' % table,
                format, state='error', code=404)
    return db[table]

def _request_query(_table, _params, format):
    """ From a set of query parameters, apply those that
    affect a query result set, e.g. sorting, limiting and 
    offsets.

    Returns a tuple of the remaining query parameters and
    a curried call to the database.
    """
    params = _params.copy()
    if '_callback' in params:
        params.pop('_callback')

    try:
        limit = int(params.pop('_limit', None)) \
                    if '_limit' in params else None
        offset = int(params.pop('_offset', None)) \
                    if '_offset' in params else None
    except ValueError, ve:
        raise WebstoreException('Invalid value: %s' % ve,
                                format, state='error', code=400)

    sorts = []
    for sort in params.poplist('_sort'):
        if not ':' in sort:
            raise WebstoreException( 
                'Invalid sorting format, use: order:column',
                format, state='error', code=400)
        order, column = sort.split(':', 1)
        order = {'asc': asc, 'desc': desc}.get(order.lower(), 'asc')
        sorts.append(order(column))

    args = {'limit': limit, 'offset': offset, 'order_by': sorts}
    return params, args

@store.route('/<user>.<format>', methods=['GET', 'OPTIONS'])
@store.route('/<user>', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def databases(user, format=None):
    """ Give a list of all databases owned by the user. """
    #require(user, database, 'read', format)
    try:
        databases = []

        # TODO: this is a hack, find a nicer way to do this
        #
        # we want to allow user names to contain a '.', eg: 'thedatahub.org'
        # this breaks the routing for this function
        #
        # so if format exists, check that it should not be part of the user name
        if format:
            try:
                user_databases = db_factory.databases_by_user(user + '.' + format)
                user = user + '.' + format
            except UserNotFound:
                user_databases = db_factory.databases_by_user(user)
        else:
            user_databases = db_factory.databases_by_user(user)
        
        for database in user_databases:
            url = url_for('webstore.index', user=user, database=database)
            databases.append({'name': database, 'url': url})
        return render_table(request, databases, ['name', 'url'], format)
    except UserNotFound:
        raise WebstoreException('User not found', format, state='error', code=404)
    except NamingException, ne:
        raise WebstoreException('Invalid name: %s' % ne.field,
                format, state='error', code=400)

@store.route('/<user>/<database>.<format>', methods=['GET', 'OPTIONS'])
@store.route('/<user>/<database>', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def index(user, database, format=None):
    """ Give a list of all tables in the database. """
    require(user, database, 'read', format)
    try:
        db = db_factory.create(user, database)
    except NamingException, ne:
        raise WebstoreException('Invalid name: %s' % ne.field,
                format, state='error', code=400)

    # send out sqlite database raw:
    if response_format(request, format) == 'db' \
            and db.engine.name == 'sqlite':
        if not os.path.isfile(db.engine.engine.url.database):
            return WebstoreException('No such database: %s' % database,
                                'json', state='error', code=404)
        log.debug("Streaming out DB: %s" % db.engine.engine.url.database)
        return send_file(db.engine.engine.url.database,
                         mimetype=SQLITE)

    tables = []
    for table in db.engine.table_names():
        url = url_for('webstore.read', user=user, database=database, table=table)
        tables.append({'name': table, 'url': url})
    return render_table(request, tables, ['name', 'url', 'columns'], format)

@store.route('/<user>/<database>.<format>', methods=['PUT'])
@store.route('/<user>/<database>', methods=['PUT'])
def sql(user, database, format=None):
    """ Execute an SQL statement on the database. """
    # TODO: do we really, really need this? 
    if request.content_type == 'text/sql':
        query = request.data
        params = None
        attaches = []
    elif request.content_type.lower() == 'application/json':
        query = request.json.get('query', '')
        params = request.json.get('params')
        attaches = request.json.get('attach', [])
    else:
        raise WebstoreException('Only text/sql, application/json is supported',
                format, state='error', code=400)
    try:
        if has(user, database, 'delete'):
            db = db_factory.create(user, database)
        else:
            require(user, database, 'read', format)
            db = db_factory.create_readonly(user, database)
    except NamingException, ne:
        raise WebstoreException('Invalid DB name: %s' % ne.field,
                format, state='error', code=400)
    try:
        connection = db.engine.connect()
        for attach in attaches:
            attach_user = attach.get('user', user)
            attach_db = attach['database']
            require(attach_user, attach_db, 'read', format)
            connection = db_factory.attach(db.authorizer, connection, 
                attach_user, attach_db, attach.get('alias', attach_db))
        params_dict = params if isinstance(params, dict) else {}
        params_list = [] if isinstance(params, dict) or not params else params
        log.debug("Query: %s (params: %s)" % (query, params))
        results = connection.execute(query, *params_list, **params_dict)
        return render_table(request, result_proxy_iterator(results), 
                            results.keys(), format)
    except NamingException, ne:
        raise WebstoreException('Invalid attach DB name: %s' % ne.field,
                format, state='error', code=400)
    except KeyError, ke:
        raise WebstoreException('Invalid attach DB: %s' % ke,
                format, state='error', code=400)
    except DatabaseError, de:
        raise WebstoreException('DB Error: %s' % de.message,
                format, state='error', code=400)

@store.route('/<user>/<database>.<format>', methods=['POST'])
@store.route('/<user>/<database>', methods=['POST'])
def create(user, database, format=None):
    """ A table name needs to specified either as a query argument
    or as part of the URL. This will forward to the URL variant. """
    if not 'table' in request.args:
        return render_message(request, 'Missing argument: table',
                format, state='error', code=400)
    return upsert(user, database, request.args.get('table'), 
                  format=format)

@store.route('/<user>/<database>/<table>.<format>', methods=['POST', 'PUT'])
@store.route('/<user>/<database>/<table>', methods=['POST', 'PUT'])
def upsert(user, database, table, format=None):
    require(user, database, 'write', format)
    try:
        db = db_factory.create(user, database)
    except NamingException, ne:
        raise WebstoreException('Invalid DB name: %s' % ne.field,
                format, state='error', code=400)
    try:
        _table = db[table]
    except NamingException, ne:
        raise WebstoreException('Invalid table name: %s' % ne.field,
                                format, state='error', code=400)
    unique = request.args.getlist('unique')
    if len(unique):
        require(user, database, 'delete', format)
    reader = read_request(request, format)
    new_count = 0
    try:
        for row in reader:
            if not len(row.keys()):
                continue
            if not _table.update_row(unique, row):
                _table.add_row(row)
            new_count += 1
    except StatementError, se:
        raise WebstoreException(unicode(se), format, state='error', 
                                code=400)
    except NamingException, ne:
        raise WebstoreException('Invalid column name: %s' % ne.field,
                                format, state='error', code=400)
    _table.commit()
    raise WebstoreException('Successfully saved: %s (%s rows)' % (table, new_count),
                format, state='success', code=201,
                url=url_for('webstore.read', user=user, database=database, table=table))

@store.route('/<user>/<database>/<table>.<format>', methods=['GET', 'OPTIONS'])
@store.route('/<user>/<database>/<table>', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def read(user, database, table, format=None):
    require(user, database, 'read', format)
    _table = _get_table(user, database, table, format)
    params, select_args = _request_query(_table, request.args,
                                         format)
    try:
        clause = _table.args_to_clause(params)
    except KeyError, ke:
        raise WebstoreException('Invalid filter: %s' % ke,
                format, state='error', code=400)
    try:
        statement = _table.table.select(clause, **select_args)
        log.debug("Read: %s" % statement)
        results = _table.bind.execute(statement)

        # produce a count 
        # TODO: make this optional?
        count_statement = select([func.count()], clause, _table.table)
        count = _table.bind.execute(count_statement).fetchone()[0]
        log.debug("Results: %s" % count)
        headers = {'X-Count': count}

    except OperationalError, oe:
        raise WebstoreException('Invalid query: %s' % oe.message,
            format, state='error', code=400)
    return render_table(request, result_proxy_iterator(results), 
                        results.keys(), format, headers=headers)

@store.route('/<user>/<database>/<table>/row/<row>.<format>', methods=['GET', 'OPTIONS'])
@store.route('/<user>/<database>/<table>/row/<row>', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def row(user, database, table, row, format=None):
    require(user, database, 'read', format)
    _table = _get_table(user, database, table, format)
    try:
        row = int(row)
    except ValueError:
        raise WebstoreException('Invalid row ID: %s' % row,
                format, state='error', code=400)
    if row == 0:
        raise WebstoreException(
            'Starting at offset 1 to allow header row',
            format, state='error', code=400)
    params, select_args = _request_query(_table, request.args,
                                         format)
    select_args['limit'] = 1
    select_args['offset'] = row-1
    try:
        statement = _table.table.select('', **select_args)
        log.debug("Read row: %s" % statement)
        results = _table.bind.execute(statement)
    except OperationalError, oe:
        raise WebstoreException('Invalid query: %s' % oe.message,
            format, state='error', code=400)
    return render_table(request, result_proxy_iterator(results), 
                        results.keys(), format)

@store.route('/<user>/<database>/<table>/schema.<format>', methods=['GET', 'OPTIONS'])
@store.route('/<user>/<database>/<table>/schema', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def schema(user, database, table, format=None):
    require(user, database, 'read', format)
    _table = _get_table(user, database, table, format)
    schema = []
    for column in _table.table.columns:
        values_url = url_for('webstore.distinct', user=user, database=database,
                             table=table, column=column.name)
        schema.append(dict(name=column.name,
                           values_url=values_url,
                           type=unicode(column.type).lower()))
    return render_table(request, schema, schema[0].keys(), format)

@store.route('/<user>/<database>/<table>/distinct/<column>.<format>',
        methods=['GET', 'OPTIONS'])
@store.route('/<user>/<database>/<table>/distinct/<column>', 
        methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def distinct(user, database, table, column, format=None):
    require(user, database, 'read', format)
    _table = _get_table(user, database, table, format)
    if not column in _table.table.columns:
        raise WebstoreException('No such column: %s' % column,
                format, state='error', code=404)
    params, select_args = _request_query(_table, request.args,
                                         format)
    select_args['distinct'] = True
    if not len(select_args['order_by']):
        select_args['order_by'].append(desc('_count'))
    try:
        col = _table.table.c[column]
        statement = select([col, func.count(col).label('_count')], 
                group_by=[col], **select_args)
        log.debug("Distinct: %s" % statement)
        results = _table.bind.execute(statement)
    except OperationalError, oe:
        raise WebstoreException('Invalid query: %s' % oe.message,
            format, state='error', code=400)
    return render_table(request, result_proxy_iterator(results), 
                        results.keys(), format)

@store.route('/<user>/<database>/<table>.<format>', methods=['DELETE'])
@store.route('/<user>/<database>/<table>', methods=['DELETE'])
def delete(user, database, table, format=None):
    require(user, database, 'delete', format)
    _table = _get_table(user, database, table, format)
    _table.drop()
    log.debug("Dropping: %s" % _table.table.name)
    _table.commit()
    raise WebstoreException('Table dropped: %s' % table,
                            format, state='success', code=410)
