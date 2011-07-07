import re

class NamingException(Exception):

    def __init__(self, field):
        self.field = field

VALID_NAME = re.compile('^[a-zA-Z][a-zA-Z0-9_]{1,254}$')

def validate_name(name):
    if VALID_NAME.match(name) is None:
        raise NamingException(name)
    return name






