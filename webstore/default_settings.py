DEBUG = False

# webserver host and port
HOST = '0.0.0.0'
PORT = 5000

SECRET = 'foobar'

SQLITE_DIR = '/tmp'

AUTHORIZATION = {
    'self': ['read', 'write', 'delete'],
    'user': ['read'],
    'world': ['read']
    }

AUTH_FUNCTION = 'always_login'
HAS_FUNCTION = 'default'
#CKAN_DB_URI = 'postgresql://okfn@localhost/ckantest'
