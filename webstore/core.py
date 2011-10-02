import os
from flask import Flask
from webstore import default_settings as settings

app = Flask(__name__)
app.config.from_object(settings)
app.config.from_envvar('WEBSTORE_SETTINGS', silent=True)
# parent directory
here = os.path.dirname(os.path.abspath( __file__ ))
config_path = os.path.join(os.path.dirname(here), 'settings_local.py')
if os.path.exists(config_path):
    app.config.from_pyfile(config_path)

