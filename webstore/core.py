from flask import Flask

from webstore import default_settings

app = Flask(__name__)
app.config.from_object(default_settings)
app.config.from_envvar('WEBSTORE_SETTINGS', silent=True)

