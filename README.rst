webstore is a web-api enabled datastore backed onto sqlite or mongodb.

Requirements
============

* Cyclone and twisted

Run the web server::

  python webstore/api.py

Run tests (start server first!)::

  nosetests test/test_api.py

API
===

Current:

/?owner=...&database=...&data={jsondict}

Options:
    /?data={json-data}
    /jsonrpc

Proposed
--------

Read
~~~~

GET: /{owner}/{db-name}/?sql=...
GET: /{owner}/{db-name}/?table=...&attr=value&attr=value&limit=...

Returns: 

{
    u'keys': [u'id', u'name'],
    u'data': [
        [1, u'jones'],
        [u'aaa', u'jones']
        ]
}

Write
~~~~~

POST to:

/{owner/{database}/{table}

Payload is json data structured as follows:

{
    unique_keys: [list of key attributes]
    data: {dict of values}
}


Authentication and Authorization
--------------------------------

Authentication: use basic auth header.


Authorization:

    * Default: all read, owner can write
    * Restricted: owner can read and write, everyone can do nothing

Possible future: config file can specify a python method (TODO: method
signature)


Integration with Other Systems
==============================

Delegate authenatication to user database in some other system.


Plan
====

* DONE. Import existing uml/dataproxy stuff as per Francis' info
* DONE. Get some tests (use existing scraperwiki frontend code)
* DONE. Replace webstore/dataproxy.py with something simpler (probably cyclone based).

