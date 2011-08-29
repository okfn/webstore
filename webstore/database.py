import os 

from sqlalchemy import create_engine
from sqlalchemy import Integer, UnicodeText
from sqlalchemy.sql import and_
from glob import iglob
from sqlalchemy.schema import Table, MetaData, Column
from migrate.versioning.util import construct_engine

from webstore.validation import validate_name

ID_COLUMN = '__id__'

class DatabaseHandler(object):
    """ Handle database-wide operations. """

    def __init__(self, engine):
        self.engine = construct_engine(engine)
        self.meta = MetaData()
        self.meta.bind = self.engine

    def _create_table(self, table_name):
        table_name = validate_name(table_name)
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
        # TODO: decide if type-guessing is a good idea here.
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
        except KeyError: # column does not exist
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


class SQLiteDatabaseHandlerFactory(DatabaseHandlerFactory):

    def _user_directory(self, user_name):
        prefix = self.app.config.get('SQLITE_DIR', '/tmp')
        user_directory = os.path.join(prefix, validate_name(user_name))
        if not os.path.isdir(user_directory):
            os.makedirs(user_directory)
        return user_directory

    def databases_by_user(self, user_name):
        user_directory = self._user_directory(user_name)
        return (os.path.basename(db).rsplit('.', 1)[0] for db in \
                iglob(user_directory + '/*.db'))

    def create(self, user_name, database_name, authorizer=None):
        user_directory = self._user_directory(user_name)
        database_name = validate_name(database_name)
        path = os.path.join(user_directory, database_name + '.db')
        def make_conn():
            import sqlite3
            conn = sqlite3.connect(path, timeout=10)
            if authorizer is not None:
                conn.set_authorizer(authorizer)
            return conn
        return DatabaseHandler(create_engine('sqlite:///' + path, 
            creator=make_conn))

    def create_readonly(self, user_name, database_name):
        import sqlite3
        def authorizer(action_code, tname, cname, sql_location, trigger):
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
            return sqlite3.SQLITE_DENY
        return self.create(user_name, database_name, authorizer)
