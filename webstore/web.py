import os 

from flask import request, url_for, g, send_file
from sqlalchemy.sql.expression import asc, desc
from sqlalchemy.sql.expression import select
from sqlalchemy import func
from sqlalchemy.exc import OperationalError

from webstore.core import app
from webstore.formats import render_table, render_message
from webstore.formats import read_request, response_format
from webstore.formats import SQLITE
from webstore.helpers import WebstoreException
from webstore.helpers import entry_point_function, crossdomain
from webstore.validation import NamingException
from webstore.security import require

def _result_proxy_iterator(rp):
    """ SQLAlchemy ResultProxies are not iterable to get a 
    list of dictionaries. This is to wrap them. """
    keys = rp.keys()
    while True:
        row = rp.fetchone()
        if row is None:
            break
        yield dict(zip(keys, row))

def _get_table(user, database, table, format):
    """ Locate a named table or raise a 404. """
    try:
        db = app.db_factory.create(user, database)
    except NamingException, ne:
        raise WebstoreException('Invalid DB name: %s' % ne.field,
                format, state='error', code=400)
    if not table in db:
        raise WebstoreException('No such table: %s' % table,
                format, state='error', code=404)
    return db[table]

@app.before_request
def jsonp_callback_register():
    # This is a slight hack to not make JSON-P callback names 
    # end up in the query string, we'll keep it around on the 
    # request global and then read it in the table generator.
    g.callback = request.args.get('_callback')

@app.before_request
def check_authentication():
    g.user = None
    if 'REMOTE_USER' in request.environ:
        g.user = request.environ['REMOTE_USER']
    if 'Authorization' in request.headers:
        # If an authentication header is present, try to decode it 
        # and pass it through the function AUTH_FUNCTION which is an 
        # entry point named in the settings file. 
        authorization = request.headers.get('Authorization')
        authorization = authorization.split(' ', 1)[-1]
        g.user, password = authorization.decode('base64').split(':', 1)
        check_function = entry_point_function(app.config['AUTH_FUNCTION'],
                                              'webstore.auth')
        if not check_function(g.user, password):
            # FIXME: we don't know the format yet.
            raise WebstoreException('Invalid username or password!', None,
                                    state='error', code=401)

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

@app.route('/<user>/<database>.<format>', methods=['GET', 'OPTIONS'])
@app.route('/<user>/<database>', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def index(user, database, format=None):
    """ Give a list of all tables in the database. """
    require(user, database, 'read', format)
    try:
        db = app.db_factory.create(user, database)
    except NamingException, ne:
        raise WebstoreException('Invalid DB name: %s' % ne.field,
                format, state='error', code=400)

    # send out sqlite database raw:
    if response_format(request, format) == 'db' \
            and db.engine.name == 'sqlite':
        if not os.path.isfile(db.engine.engine.url.database):
            return WebstoreException('No such database: %s' % database,
                                'json', state='error', code=404)
        return send_file(db.engine.engine.url.database,
                         mimetype=SQLITE)

    tables = []
    for table in db.engine.table_names():
        url = url_for('read', user=user, database=database, table=table)
        tables.append({'name': table, 'url': url})
    return render_table(request, tables, ['name', 'url', 'columns'], format)

@app.route('/<user>/<database>.<format>', methods=['PUT'])
@app.route('/<user>/<database>', methods=['PUT'])
def sql(user, database, format=None):
    """ Execute an SQL statement on the database. """
    # TODO: do we really, really need this? 
    require(user, database, 'delete', format)
    if request.content_type != 'text/sql':
        raise WebstoreException('Only text/sql content is supported',
                format, state='error', code=400)
    try:
        db = app.db_factory.create(user, database)
    except NamingException, ne:
        raise WebstoreException('Invalid DB name: %s' % ne.field,
                format, state='error', code=400)
    results = db.engine.execute(request.data)
    return render_table(request, _result_proxy_iterator(results), 
                        results.keys(), format)

@app.route('/<user>/<database>.<format>', methods=['POST'])
@app.route('/<user>/<database>', methods=['POST'])
def create(user, database, format=None):
    """ A table name needs to specified either as a query argument
    or as part of the URL. This will forward to the URL variant. """
    if not 'table' in request.args:
        return render_message('Missing argument: table',
                format, state='error', code=400)
    return create_named(user, database, request.args.get('table'), 
                        format=format)

@app.route('/<user>/<database>/<table>.<format>', methods=['POST'])
@app.route('/<user>/<database>/<table>', methods=['POST'])
def create_named(user, database, table, format=None):
    require(user, database, 'write', format)
    try:
        db = app.db_factory.create(user, database)
    except NamingException, ne:
        raise WebstoreException('Invalid DB: %s' % ne.field,
                format, state='error', code=400)
    if table in db:
        raise WebstoreException('Table already exists: %s' % table,
                format, state='error', code=409, 
                url=url_for('read', user=user, database=database, table=table))
    try:
        _table = db[table]
    except NamingException, ne:
        raise WebstoreException('Invalid table name: %s' % ne.field,
                                format, state='error', code=400)
    reader = read_request(request, format)
    try:
        for row in reader:
            if len(row.keys()):
                _table.add_row(row)
    except NamingException, ne:
        raise WebstoreException('Invalid column name: %s' % ne.field,
                                format, state='error', code=400)
    _table.commit()
    raise WebstoreException('Successfully created: %s' % table,
                format, state='success', code=201,
                url=url_for('read', user=user, database=database, table=table))

@app.route('/<user>/<database>/<table>.<format>', methods=['GET', 'OPTIONS'])
@app.route('/<user>/<database>/<table>', methods=['GET', 'OPTIONS'])
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
        results = _table.bind.execute(statement)

        # produce a count 
        # TODO: make this optional?
        count_statement = select([func.count()], clause, _table.table)
        count = _table.bind.execute(count_statement).fetchone()[0]
        headers = {'X-Count': count}

    except OperationalError, oe:
        raise WebstoreException('Invalid query: %s' % oe.message,
            format, state='error', code=400)
    return render_table(request, _result_proxy_iterator(results), 
                        results.keys(), format, headers=headers)

@app.route('/<user>/<database>/<table>/row/<row>.<format>', methods=['GET', 'OPTIONS'])
@app.route('/<user>/<database>/<table>/row/<row>', methods=['GET', 'OPTIONS'])
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
        results = _table.bind.execute(statement)
    except OperationalError, oe:
        raise WebstoreException('Invalid query: %s' % oe.message,
            format, state='error', code=400)
    return render_table(request, _result_proxy_iterator(results), 
                        results.keys(), format)

@app.route('/<user>/<database>/<table>/schema.<format>', methods=['GET', 'OPTIONS'])
@app.route('/<user>/<database>/<table>/schema', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def schema(user, database, table, format=None):
    require(user, database, 'read', format)
    _table = _get_table(user, database, table, format)
    schema = []
    for column in _table.table.columns:
        values_url = url_for('distinct', user=user, database=database,
                             table=table, column=column.name)
        schema.append(dict(name=column.name,
                           values_url=values_url,
                           type=unicode(column.type).lower()))
    return render_table(request, schema, schema[0].keys(), format)

@app.route('/<user>/<database>/<table>/distinct/<column>.<format>',
        methods=['GET', 'OPTIONS'])
@app.route('/<user>/<database>/<table>/distinct/<column>', 
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
        results = _table.bind.execute(statement)
    except OperationalError, oe:
        raise WebstoreException('Invalid query: %s' % oe.message,
            format, state='error', code=400)
    return render_table(request, _result_proxy_iterator(results), 
                        results.keys(), format)


@app.route('/<user>/<database>/<table>.<format>', methods=['PUT'])
@app.route('/<user>/<database>/<table>', methods=['PUT'])
def update(user, database, table, format=None):
    require(user, database, 'write', format)
    _table = _get_table(user, database, table, format)
    unique = request.args.getlist('unique')
    if len(unique):
        require(user, database, 'delete', format)
    _table = _get_table(user, database, table, format)
    reader = read_request(request, format)
    try:
        for row in reader:
            if not len(row.keys()):
                continue
            if not _table.update_row(unique, row):
                _table.add_row(row)
    except NamingException, ne:
        raise WebstoreException('Invalid column name: %s' % ne.field,
                                format, state='error', code=400)
    _table.commit()
    raise WebstoreException('Table updated: %s' % table,
                            format, state='success', code=201,
                            url=url_for('read', user=user, database=database, table=table))

@app.route('/<user>/<database>/<table>.<format>', methods=['DELETE'])
@app.route('/<user>/<database>/<table>', methods=['DELETE'])
def delete(user, database, table, format=None):
    require(user, database, 'delete', format)
    _table = _get_table(user, database, table, format)
    _table.drop()
    _table.commit()
    raise WebstoreException('Table dropped: %s' % table,
                            format, state='success', code=410)

@app.route('/login')
def login():
    """ Helper function to provoke authorization via the browser. """
    if g.user:
        return render_message(request, '', format, state='success', url='/', 
                              code=302)
    response = render_message(request, 'Please authenticate',
                              format, state='success', code=401)
    response.headers['WWW-Authenticate'] = 'Basic realm="WebStore access"'
    return response


if __name__ == "__main__":
    app.run()

