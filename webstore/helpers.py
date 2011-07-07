import pkg_resources

from flask import request
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


