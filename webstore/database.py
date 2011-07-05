import os 

from sqlalchemy import create_engine
from sqlalchemy import Integer, UnicodeText
from sqlalchemy.sql import and_
from sqlalchemy.schema import Table, MetaData, Column
from migrate.versioning.util import construct_engine

ID_COLUMN = '__id__'

class DatabaseHandler(object):
    """ Handle database-wide operations. """

    def __init__(self, engine):
        self.engine = construct_engine(engine)
        self.meta = MetaData()
        self.meta.bind = self.engine

    def _create_table(self, table_name):
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

    def _ensure_columns(self, row):
        for column in set(row.keys()) - set(self.table.columns.keys()):
            _type = self._guess_type(column, row[column])
            col = Column(column, _type)
            col.create(self.table)

    def add_row(self, row):
        """ Add a row (type: dict). If any of the keys of
        the row are not table columns, they will be type
        guessed and created.
        """
        self._ensure_columns(row)
        stmt = self.table.insert(row)
        self.bind.execute(stmt)

    def args_to_clause(self, args):
        clauses = []
        for k, v in args.items():
            clauses.append(self.table.c[k] == v)
        return and_(*clauses)

    def update_row(self, whereclause, row):
        """ Update a row (type: dict) based on whereclause.

        If any of the keys of the row are not table columns, they will 
        be type guessed and created.
        """
        #TODO: analyze whereclause and generate index?
        self._ensure_columns(row)
        stmt = self.table.update(whereclause, row)
        self.bind.execute(stmt)

class DatabaseHandlerFactory(object):
    """ An engine factory will generate a database with
    the given name and return an SQLAlchemy engine bound
    to it.
    """

    def __init__(self, app):
        self.app = app

    def create(self, name):
        pass


class SQLiteDatabaseHandlerFactory(DatabaseHandlerFactory):

    def create(self, name):
        prefix = self.app.config.get('SQLITE_DIR', '/tmp')
        name = name.replace('.', '')
        path = os.path.join(prefix, name + '.db')
        return DatabaseHandler(create_engine('sqlite:///' + path))


