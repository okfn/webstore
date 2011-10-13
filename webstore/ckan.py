"""
Here be dragons. 

Go into the innards of CKAN and look for fun stuff (i.e. user, password)
"""
import logging

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import Table, MetaData

from webstore.core import app
from webstore.helpers import WebstoreException

log = logging.getLogger(__name__)

def check_hashed_password(password, db_password):
    """ This is copied from CKAN and need to be kept up to date. """
    if not password or not db_password: 
        return False
    if isinstance(password, unicode):
        password_8bit = password.encode('ascii', 'ignore')
    else:
        password_8bit = password
    hashed_pass = sha1(password_8bit + db_password[:40])
    return db_password[40:] == hashed_pass.hexdigest()

def check_ckan_login(request):
    """ Connect to a specified CKAN database via SQLAlchemy and 
    try to find the user that is authenticating. 
    """
    db_uri = app.config.get('CKAN_DB_URI')
    if db_uri is None:
        log.warn("No CKAN_DB_URI given, cannot authenticate!")
        return False
    if 'Authorization' in request.headers:
        apikey = request.headers.get('Authorization')
        engine = create_engine(db_uri, poolclass=NullPool)
        meta = MetaData()
        meta.bind = engine
        table = Table('user', meta, autoload=True)
        results = engine.execute(table.select(table.c.apikey==apikey))
        # TODO: check for multiple matches, never trust ckan.
        record = results.first()
        if record is not None:
            return record['name']
        raise WebstoreException('Invalid apikey!', None,
                                state='error', code=401)
    return None


    
