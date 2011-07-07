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
from webstore.core import app

def has(user, database, action):
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



