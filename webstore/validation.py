import re

class NamingException(Exception):

    def __init__(self, field):
        self.field = field

VALID_NAME = re.compile('^[a-zA-Z0-9][a-zA-Z0-9_]{0,254}$')
VALID_DBNAME = re.compile('^[a-zA-Z0-9][a-zA-Z0-9_\-]{0,254}$')
VALID_USERNAME = re.compile('^[a-zA-Z0-9][a-zA-Z0-9_\-\.]{0,254}$')

def validate_name(name):
    if VALID_NAME.match(name) is None:
        raise NamingException(name)
    return name

def validate_dbname(name):
    if VALID_DBNAME.match(name) is None:
        raise NamingException(name)
    return name

def validate_username(name):
    if VALID_USERNAME.match(name) is None:
        raise NamingException(name)
    return name

def validate_columnname(name):
    if name.startswith('_'):
        raise NamingException(name)
    return name

