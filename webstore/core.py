from flask import Flask

from webstore import default_settings
from webstore.database import SQLiteDatabaseHandlerFactory

app = Flask(__name__)
app.config.from_object(default_settings)
app.config.from_envvar('WEBSTORE_SETTINGS', silent=True)

# TODO: use entry points and config for IoC.
app.db_factory = SQLiteDatabaseHandlerFactory(app)

