import logging

from flask import request, g, render_template

from webstore.core import app
from webstore.helpers import entry_point_function
from webstore.formats import render_message

from webstore.views import store

@app.before_request
def check_authentication():
    check_function = entry_point_function(app.config['AUTH_FUNCTION'],
                                          'webstore.auth')
    g.user = check_function(request)

@app.route('/login')
def login():
    """ Helper function to provoke authorization via the browser. """
    if g.user:
        return render_message(request, '', format, state='success', url='/', 
                              code=302)
    response = render_message(request, 'Please authenticate',
                              format, state='success', code=401)
    response.headers['WWW-Authenticate'] = 'Basic realm="WebStore access"'
    return response

@app.route('/')
def home():
    return render_template('index.html')

app.register_blueprint(store)

if __name__ == "__main__":
    logging.basicConfig(level=logging.NOTSET)
    app.run()

