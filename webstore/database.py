import os 
from glob import iglob
import logging

from sqlalchemy import create_engine
from sqlalchemy import Integer, UnicodeText, Float
from sqlalchemy.sql import and_
from sqlalchemy.schema import Table, MetaData, Column
from migrate.versioning.util import construct_engine

from webstore.validation import validate_name, validate_dbname, validate_username

log = logging.getLogger(__name__)
ID_COLUMN = '__id__'

class DatabaseHandler(object):
    """ Handle database-wide operations. """

    def __init__(self, engine):
        self.engine = construct_engine(engine)
        self.meta = MetaData()
        self.meta.bind = self.engine

    def _create_table(self, table_name):
        table_name = validate_name(table_name)
        log.debug("Creating table: %s on %r" % (table_name, self.engine))
        table = Table(table_name, self.meta)
        col = Column(ID_COLUMN, Integer, primary_key=True)
        table.append_column(col)
        table.create(self.engine)
        return table

    def _load_table(self, table_name):
        return Table(table_name, self.meta, autoload=True)

    def __contains__(self, table_name):
        """ Check if the given table exists. """
        return self.engine.has_table(table_name)

    def __getitem__(self, table_name):
        """ return a TableHandler for the named table.
        If the table does not exist, create it. """
        if not table_name in self:
            table = self._create_table(table_name)
        else:
            table = self._load_table(table_name)
        return TableHandler(table, self.engine, self.meta)

    def finalize(self):
        self.engine.dispose()

class TableHandler(object):
    """ Handle operations on tables. """

    def __init__(self, table, engine, meta):
        self.table = table
        self.bind = engine.connect()
        self.tx = self.bind.begin()
        self.meta = meta

    def commit(self):
        self.tx.commit()

    def drop(self): 
        """ DROP the table. """
        self.table.drop()

    def _guess_type(self, column, sample):
        if isinstance(sample, int):
            return Integer
        elif isinstance(sample, float):
            return Float
        return UnicodeText

    def _type_convert(self, row):
        _row = []
        for k, v in row.items():
            if v is None:
                _row.append((k, v))
            else:
                _row.append((k, unicode(v)))
        return dict(_row)

    def _ensure_columns(self, row):
        columns = set(row.keys()) - set(self.table.columns.keys())
        columns = map(validate_name, columns)
        for column in columns:
            _type = self._guess_type(column, row[column])
            log.debug("Creating column: %s (%s) on %r" % (column, 
                _type, self.table.name))
            col = Column(column, _type)
            col.create(self.table)

    def add_row(self, row):
        """ Add a row (type: dict). If any of the keys of
        the row are not table columns, they will be type
        guessed and created.
        """
        self._ensure_columns(row)
        row = self._type_convert(row)
        self.bind.execute(self.table.insert(row))

    def args_to_clause(self, args):
        clauses = []
        for k, v in args.items():
            clauses.append(self.table.c[k] == v)
        return and_(*clauses)

    def update_row(self, unique, row):
        """ Update a row (type: dict) based on the unique keys.

        If any of the keys of the row are not table columns, they will 
        be type guessed and created.
        """
        if not len(unique):
            return False
        clause = dict([(u, row.get(u)) for u in unique])
        self._ensure_columns(row)
        row = self._type_convert(row)
        try:
            stmt = self.table.update(self.args_to_clause(clause), row)
            rp = self.bind.execute(stmt)
            return rp.rowcount > 0
        except KeyError, ke:
            log.warn("UPDATE: filter column does not exist: %s" % ke)
            return False

class DatabaseHandlerFactory(object):
    """ An engine factory will generate a database with
    the given name and return an SQLAlchemy engine bound
    to it.
    """

    def __init__(self, app):
        self.app = app

    def databases_by_user(self, user_name):
        pass

    def create(self, user_name, database_name):
        pass

    def create_readonly(self, user_name, database_name):
        pass

    def attach(self, authorizer, connection, user_name, database_name, alias):
        pass


import sqlite3
def authorizer_ro(action_code, tname, cname, sql_location, trigger):
    #print action_code, tname, cname, sql_location, trigger
    # thanks to ScraperWiki 
    #print (action_code, tname, cname, sql_location, trigger)
    readonlyops = [ sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ,
                    sqlite3.SQLITE_DETACH, 31, 19 ]
    # 31=SQLITE_FUNCTION missing from library.
    # codes: http://www.sqlite.org/c3ref/c_alter_table.html
    if action_code in readonlyops:
        return sqlite3.SQLITE_OK
    if action_code == sqlite3.SQLITE_PRAGMA:
        if tname in ["table_info", "index_list", "index_info"]:
            return sqlite3.SQLITE_OK
    log.debug("Unauthorized query: %s / %s / %c " % (action_code, tname, cname))
    return sqlite3.SQLITE_DENY

def authorizer_attach(action_code, tname, cname, sql_location, trigger):
    #print action_code, tname, cname, sql_location, trigger
    if action_code == sqlite3.SQLITE_ATTACH: 
        return sqlite3.SQLITE_OK
    return authorizer_ro(action_code, tname, cname, sql_location, trigger)

def authorizer_rw(action_code, tname, cname, sql_location, trigger):
    #print action_code, tname, cname, sql_location, trigger
    if sql_location == None or sql_location == 'main':  
        return sqlite3.SQLITE_OK
    return authorizer_ro(action_code, tname, cname, sql_location, trigger)

class SQLiteDatabaseHandlerFactory(DatabaseHandlerFactory):

    def _user_directory(self, user_name):
        prefix = self.app.config.get('SQLITE_DIR', '/tmp')
        user_directory = os.path.join(prefix, validate_username(user_name))
        if not os.path.isdir(user_directory):
            os.makedirs(user_directory)
        return user_directory

    def databases_by_user(self, user_name):
        user_directory = self._user_directory(user_name)
        log.debug("Directory listing: %s" % user_directory)
        return (os.path.basename(db).rsplit('.', 1)[0] for db in \
                iglob(user_directory + '/*'))

    def database_path(self, user_name, database_name):
        database_name = validate_dbname(database_name)
        db_directory =  os.path.join(self._user_directory(user_name),
                                     database_name)

        # TODO: workaround for old-format stores.
        if os.path.isfile(db_directory  + '.db'):
            return db_directory  + '.db'

        if not os.path.isdir(db_directory):
            os.makedirs(db_directory)
        return os.path.join(db_directory, 'defaultdb.sqlite')

    def create(self, user_name, database_name, authorizer=authorizer_rw):
        path = self.database_path(user_name, database_name)
        def make_conn():
            conn = sqlite3.connect(path, timeout=10)
            if authorizer is not None:
                conn.set_authorizer(authorizer)
            return conn
        log.debug("Loading SQLite DB: %s" % path)
        handler = DatabaseHandler(create_engine('sqlite:///' + path, 
            creator=make_conn))
        handler.authorizer = authorizer
        return handler

    def create_readonly(self, user_name, database_name):
        return self.create(user_name, database_name, authorizer_ro)

    def attach(self, authorizer, connection, user_name, database_name, alias):
        """ Attach a database to another, existing database. This means all
        tables within the other database will become available with a prefix,
        ``alias``. In order to attach safely, we need to temporarily swap 
        out the authorization function of the connection to allow the attach 
        operation. """
        connection.connection.set_authorizer(authorizer_attach)
        path = self.database_path(user_name, database_name)
        log.debug("Attaching SQLite DB: %s (as %s)" % (path, alias))
        connection.execute("ATTACH DATABASE ? AS ?", path, alias)
        connection.connection.set_authorizer(authorizer)
        return connection
