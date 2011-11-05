webstore is a RESTful data store for tabular and table-like data. It can
be used as a dynamic storage for table data, allowing filtered, partial 
or full retrieval and format conversion.

API Documentation
=================

Documentation on the API is in ``doc/index.rst`` and on Read The Docs
(http://webstore.readthedocs.org/en/latest/).

Client Libraries
================

The API is build around a standard, simple RESTful JSON core so using it is
just a couple of lines from most languages. Nevertheless, for additional
convenience client libraries are available. Currently there is:

 * Python: http://github.com/okfn/webstore-client

Installation and Usage
======================

Install the code and requirements (you may wish to create a virtualenv first)::

  pip install webstore
  # OR: if you want to install from source
  git clone https://github.com/okfn/webstore
  cd webstore
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

Production Deployment
---------------------

This will vary from system to system but here are some tips. Basic setup is as
for installation.

Sample WSGI file::

  import os, sys
  sys.stdout = sys.stderr
  os.environ['WEBSTORE_SETTINGS'] = '/path/to/settings.py'
  # this assumes you have installed into virtualenv
  instance_dir = '/path/to/virtualenv'
  pyenv_bin_dir = os.path.join(instance_dir, 'bin')
  activate_this = os.path.join(pyenv_bin_dir, 'activate_this.py')
  execfile(activate_this, dict(__file__=activate_this))
  from webstore.web import app as application

Database directory: in your settings you will have specified a database
directory. Make sure this is readable and writable by the web server user.


Authentication Integration
==========================

This is discussed further in the docs but we note here that
settings_local.py.tmpl provides standard authentication setup plus example on
how to connect to a CKAN instance.

