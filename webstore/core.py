from flask import Flask

try:
    from webstore import settings
except ImportError:
    from webstore import default_settings as settings

app = Flask(__name__)
app.config.from_object(settings)
app.config.from_envvar('WEBSTORE_SETTINGS', silent=True)

