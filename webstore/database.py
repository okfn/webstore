import os 

from flask import g
from sqlalchemy import create_engine
from sqlalchemy import Integer, UnicodeText
from sqlalchemy import event
from sqlalchemy.sql import and_, text
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import Table, MetaData, Column
from migrate.versioning.util import construct_engine

from webstore.validation import validate_name

ID_COLUMN = '__id__'

class DatabaseHandler(object):
    """ Handle database-wide operations. """

    def __init__(self, engine, schema=None, 
            table_callback=lambda t: t):
        self.engine = construct_engine(engine)
        self.meta = MetaData()
        self.schema = schema
        self.table_callback = table_callback
        self.meta.bind = self.engine

    def _create_table(self, table_name):
        table_name = validate_name(table_name)
        table = Table(table_name, self.meta, 
                      schema=self.schema)
        self.table_callback(table)
        col = Column(ID_COLUMN, Integer, primary_key=True)
        table.append_column(col)
        table.create(self.engine)
        return table

    def _load_table(self, table_name):
        table = Table(table_name, self.meta, 
                     schema=self.schema,
                     autoload=True)
        self.table_callback(table)
        return table

    def __contains__(self, table_name):
        """ Check if the given table exists. """
        if self.schema:
            result = self.engine.execute(
                text("select 1 from pg_tables where schemaname = :s "
                     "and tablename = :t"),
                t=table_name, s=self.schema).fetchone()
            return True if result else False
        else:
            return self.engine.has_table(table_name)

    def __getitem__(self, table_name):
        """ return a TableHandler for the named table.
        If the table does not exist, create it. """
        if not table_name in self:
            table = self._create_table(table_name)
        else:
            table = self._load_table(table_name)
        return TableHandler(table, self.engine, self.meta)

    def get_tables(self):
        if self.schema:
            result = self.engine.execute(
                text("select tablename from pg_tables where schemaname = :s"),
                s=self.schema).fetchall()
            return [row[0] for row in result]
        else:
            return self.engine.table_names()

    def finalize(self):
        self.engine.dispose()

class TableHandler(object):
    """ Handle operations on tables. """

    def __init__(self, table, engine, meta):
        self.table = table
        self.bind = engine.connect()
        self.tx = self.bind.begin()

        if not hasattr(g, 'objects_to_close'):
            g.objects_to_close = []
        g.objects_to_close.append(self.tx)

        self.meta = meta

    def commit(self):
        self.tx.commit()
        self.bind.close()

    def drop(self): 
        """ DROP the table. """
        self.table.drop()

    def _guess_type(self, column, sample):
        # TODO: decide if type-guessing is a good idea here.
        return UnicodeText

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
        stmt = self.table.update(self.args_to_clause(clause), row)
        rp = self.bind.execute(stmt)
        return rp.rowcount > 0

class DatabaseHandlerFactory(object):
    """ An engine factory will generate a database with
    the given name and return an SQLAlchemy engine bound
    to it.
    """

    def __init__(self, app):
        self.app = app

    def create(self, user_name, database_name):
        pass
    
    def create_readonly(self, user_name, database_name):
        return self.create(user_name, database_name)

class PgSQLDatabaseHandlerFactory(DatabaseHandlerFactory):

    URL = 'postgresql://%s@%s/%s'

    def __init__(self, app):
        self.app = app

        self.db = self.app.config.get('PGSQL_DB', 'webstore')
        self.host = self.app.config.get('PGSQL_HOST', 'localhost')
        self.user = self.app.config.get('PGSQL_USER', 'webstore:foo')
        url = self.URL % (self.user, self.host, self.db)
        self.engine = create_engine(url, poolclass=NullPool, echo=True)

    def credentials(self, user_name, database_name):
        name = self.db + 'readonly' + user_name + '' + database_name
        passwd = self.app.secret_key or "foo"
        return name, passwd

    def setup(self, schema, user, passwd):
        conn = self.engine.connect()
        result = conn.execute(text("select 1 from pg_namespace where nspname = :s"),
                     s=schema).fetchone()
        
        if not result:
            conn.execute("CREATE SCHEMA \"%s\";" % schema)

        result = conn.execute(text("select 1 from pg_user where usename = :s"),
                     s=user).fetchone()

        if not result:
            conn.execute(text("CREATE USER \"%s\" WITH PASSWORD :p;" % user), 
                          p = passwd)

        conn.execute("BEGIN; GRANT USAGE ON SCHEMA \"%s\" TO \"%s\"; COMMIT;" % 
                (schema, user))

    def create_readonly(self, user_name, database_name):
        database_name = validate_name(database_name)
        schema = user_name + '.' + database_name
        ro_user, ro_pass = self.credentials(user_name, database_name)
        #self.setup(schema, ro_user, ro_pass)
        login = ro_user + ':' + ro_pass
        url = self.URL % (login, self.host, self.db)
        engine = create_engine(url, poolclass=NullPool, echo=True)
        return DatabaseHandler(engine, schema=schema)

    def create(self, user_name, database_name):
        database_name = validate_name(database_name)
        schema = user_name + '.' + database_name
        ro_user, ro_pass = self.credentials(user_name, database_name)
        self.setup(schema, ro_user, ro_pass)
        def after_create(target, connection, **kw):
            connection.execute("BEGIN; GRANT SELECT ON \"%s\".\"%s\" TO \"%s\"; COMMIT;" %
                    (schema, target.name, ro_user))
        def table_callback(table):
            event.listen(table, "after_create", after_create)
        return DatabaseHandler(self.engine, schema=schema,
                table_callback=table_callback)


class SQLiteDatabaseHandlerFactory(DatabaseHandlerFactory):

    def create(self, user_name, database_name):
        prefix = self.app.config.get('SQLITE_DIR', '/tmp')
        user_directory =os.path.join(prefix, validate_name(user_name))
        if not os.path.isdir(user_directory):
            os.makedirs(user_directory)
        database_name = validate_name(database_name)
        path = os.path.join(user_directory, database_name + '.db')
        return DatabaseHandler(create_engine('sqlite:///' + path))

