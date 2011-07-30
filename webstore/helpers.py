import pkg_resources
from datetime import timedelta
from functools import update_wrapper

from flask import make_response, request, current_app
from werkzeug.exceptions import HTTPException

from webstore.formats import render_message

class WebstoreException(HTTPException):
    """ Cancel abortion of the current task and return with
    the given message and error code. """

    def __init__(self, message, format, state='success', 
                 code=200, url=None):
        self.response = render_message(request, message, 
                 format, state=state, code=code, url=url)

    def get_response(self, environ):
        return self.response

def entry_point_function(name, group):
    """ Load a named function from a given entry point group. """
    for ep in pkg_resources.iter_entry_points(group, name.strip().lower()):
        return ep.load()

# from http://flask.pocoo.org/snippets/56/
def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

