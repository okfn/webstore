from flask import request, g

from webstore.core import app
from webstore.helpers import WebstoreException
from webstore.helpers import entry_point_function
from webstore.formats import render_message

from webstore.views import store

@app.before_request
def check_authentication():
    g.user = None
    if 'REMOTE_USER' in request.environ:
        g.user = request.environ['REMOTE_USER']
    if 'Authorization' in request.headers:
        # If an authentication header is present, try to decode it 
        # and pass it through the function AUTH_FUNCTION which is an 
        # entry point named in the settings file. 
        authorization = request.headers.get('Authorization')
        authorization = authorization.split(' ', 1)[-1]
        g.user, password = authorization.decode('base64').split(':', 1)
        check_function = entry_point_function(app.config['AUTH_FUNCTION'],
                                              'webstore.auth')
        if not check_function(g.user, password):
            # FIXME: we don't know the format yet.
            raise WebstoreException('Invalid username or password!', None,
                                    state='error', code=401)

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

app.register_blueprint(store)

if __name__ == "__main__":
    app.run()

