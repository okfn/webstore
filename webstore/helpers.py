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
