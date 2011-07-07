
DEBUG = True
SECRET = 'foobar'

SQLITE_DIR = '/tmp'

AUTHORIZATION = {
    'self': ['read', 'write', 'delete'],
    'user': ['read'],
    'world': ['read']
    }

AUTH_FUNCTION = 'never_login'
