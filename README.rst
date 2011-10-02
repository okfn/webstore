webstore is a RESTful data store for tabular and table-like data. It can
be used as a dynamic storage for table data, allowing filtered, partial 
or full retrieval and format conversion.

Installation and Usage
======================

Install the code and requirements (you may wish to create a virtualenv first)::

  # install webstore code
  pip install -e .

[Optional] Add to or override the default
settings (in webstore/default_settings.py)::

    cp settings_local.py.tmpl settings_local.py by copying the provided template
    # alternatively, if you want your config elsewhere or with different name
    cp settings_local.py.tmpl {/my/config/file/somewhere}
    export WEBSTORE_SETTINGS={/my/config/file/somewhere}

Run the web server::

  python webstore/web.py

Run tests (start server first!)::

  python test/test_rest.py


API Documentation
=================

Documentation on the API is in ``doc/index.rst`` and on Read The Docs (http://webstore.readthedocs.org/en/latest/).

Client Libraries
================

 * Python: http://github.com/okfn/webstore-client

Integration with Other Systems
==============================

TODO: Specify how to delegate authenatication to user database in some other system.

ScraperWiki
-----------

We could implement ScraperWikis RPC API as an extension in order to
allow scrapers to write to the store directly::

  /{user-name}/{db-name}/_swrpc?owner=...&database...&data={jsondict}

Alternatively, we could implement a 'slurper' that downloads ScraperWiki 
result data and loads it into webstore.

