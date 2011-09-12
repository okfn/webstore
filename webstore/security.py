"""
Very simple authorization system. 

The basic idea is that any request must come from a user in one of the 
following three groups:

    * 'world': anonymous visitors
    * 'user': logged-in users
    * 'self': users who want to access their own resources

For each of these events, a set of actions is queried to see if the user
is allowed to perform a given query.
"""

from flask import g

from webstore.helpers import WebstoreException
from webstore.helpers import entry_point_function
from webstore.core import app


def has(user, database, action):
    has_function = entry_point_function(app.config['HAS_FUNCTION'],
                                        'webstore.authz')
    return has_function(user, database, action)


def default_has(user, database, action):
    matrix = app.config['AUTHORIZATION']
    if user == g.user:
        capacity = 'self'
    elif g.user is not None:
        capacity = 'user'
    else:
        capacity = 'world'
    capacity_actions = matrix[capacity]
    return action in capacity_actions


def require(user, database, action, format):
    """ Require the current user to have the right to 
    execute `action` on `database` of `user`. If this 
    right is not given, raise an exception. 
    """
    if not has(user, database, action):
        raise WebstoreException('No permission to %s %s' % (action, database),
                format, state='error', code=403)


# These are for testing and can be used as mock authentication handlers.
def always_login(request):
    if 'Authorization' in request.headers:
        authorization = request.headers.get('Authorization')
        authorization = authorization.split(' ', 1)[-1]
        user, password = authorization.decode('base64').split(':', 1)
        return user
    return request.environ.get('REMOTE_USER')

def never_login(request):
    if 'Authorization' in request.headers:
        raise WebstoreException('Invalid username or password!', None,
                                state='error', code=401)
    return None

