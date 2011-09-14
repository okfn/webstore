"""
ScraperWiki Authentication

Checking read and write permissions against ScraperWiki has two cases: 

* During scraper run-time, a scraper can try to read or write a specific
  database or attach the databases of other scrapers to it to query across
  them.

* When called via the API, the webstore must check with the ScraperWiki 
  Django application if a given username or API key is valid and perhaps
  cache the result. This can be used by the Django application to extend 
  its data graph of attachable databases for each scraper.

"""
from hashlib import sha256
from urlparse import urljoin
import urllib
import logging

from flask import current_app
from webstore.lru import LRUTimeoutCache
from webstore.helpers import WebstoreException

log = logging.getLogger(__name__)
cache = LRUTimeoutCache(10000)

def sw_auth(request):
    """ Authenticate an incoming request. """
    current_app.sw_scrapername = request.headers.get('X-Scrapername')
    if current_app.sw_scrapername is not None:
        answer = sha256(current_app.sw_scrapername + \
                        current_app.config['SW_SECRET']).hexdigest()
        candidate = request.headers.get('X-Scraper-Verified')
        if candidate != answer:
            raise WebstoreException('Invalid ScraperWiki verification!', None,
                                state='error', code=401)

def sw_has(user, database, action):
    """ Authorize a specific action on a given database. """
    sw_scrapername = current_app.sw_scrapername
    if sw_scrapername == database:
        return True
    cache_key = (sw_scrapername, database)
    if cache_key in cache:
        return cache[cache_key]
    url = urljoin(current_app.config['SW_URL'], 'webstoreauth')
    query = '?scrapername=%s&attachtoname=%s' % (
            urllib.quote(sw_scrapername or ''),
            urllib.quote(database or ''))
    try:
        uh = urllib.urlopen(url + query)
        # not actual JSON:
        result = "{'attach':'Ok'}"==uh.read().strip()
        #response = json.loads(uh.read())
        #result = response['attach'] == 'ok'
        uh.close()
        cache[cache_key] = result
        return result
    except Exception, e:
        log.exception(e)
        return False







