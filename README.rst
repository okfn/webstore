webstore is a RESTful data store for tabular and table-like data. It can
be used as a dynamic storage for table data, allowing filtered, partial 
or full retrieval and format conversion.

Requirements
============

* Flask

Run the web server::

  python webstore/web.py

Run tests (start server first!)::

  python test/test_rest.py

API
===

Core Resource::

    /db/{db-name}/{table-name}

Table is the central exposed resource (databases are created on-the-fly
and may in fact not actually be seperate databases, depending on the 
backend vendor (e.g. we can use PostgreSQL Schemas to partition the
table space and don't actually need to create distinct databases).

On the ``table`` resource, the following operations are supported.

Retrieval::

  GET /db/{db-name}/{table-name}

Will read data. The desired representation should be specified as an
``Accept`` header (text/csv, application/json or text/html). As a
fallback, a file type suffix can also be used::

  GET /db/{db-name}/{table-name}.csv

The resource can also be filtered::

  GET /db/{db-name}/{table-name}?column=value

To limit the number of results or to specfiy an offset, use these query
parameters::

  GET /db/{db-name}/{table-name}?_limit=10&_offset=20

The query can also be sorted, either as 'asc' (ascending order) or 'desc'
(descending order)::

  GET /db/{db-name}/{table-name}?_sort=asc:amount

Note. It might be tempting to use '_asc' and '_desc' instead, but order
is relevant and not provided for mixed query argument names in Werkzeug.

For reference, one can also address each row of a given table at the
following location::

  GET /db/{db-name}/{table-name}/row/{line-number}

Writing
-------

To create a new table, simply POST to the database::

  POST /db/{db-name}?table={table-name}

The request must have an appropriate ``Content-type`` set. The entire
request body is treated as payload. The desired table name is either
given as a query parameter (see above) or by posting to a non-existent
table::

  POST /db/{db-name}/{table-name}

If application/json is specified as the content type, webstore will 
expect a list of single-level hashes::

  [
    {"column": "value", "other_column": "other value"},
    {"column": "banana", "other_column": "split"}
  ]

To insert additional rows into a table or to update existing rows, 
issue a PUT request with the same type of payload used for table
creation::

  PUT /db/{db-name}/{table-name}

Without further arguments, this will insert new rows as necessary.
If you want to update existing records, name the columns which are
sufficient to uniquely identify the row(s) to be updated::

  PUT /db/{db-name}/{table-name}?unique=id_colum&unique=date

This will attempt to update the database and only create a new row
if the update did not affect any existing records.

To delete an entire table, simply issue an HTTP DELETE request::

  DELETE /db/{db-name}/{table-name}

Please consider carefully before doing so because datakrishna gets angry
when people delete data.

Options (future development)
----------------------------

We could implement ScraperWikis RPC API as an extension in order to
allow scrapers to write to the store directly::

    /db/{db-name}/_swrpc?owner=...&database...&data={jsondict}

Alternatively, we could implement a 'slurper' that downloads ScraperWiki 
result data and loads it into webstore.

Executing raw SQL
-----------------

Webstore has an experimental feature to execute raw SQL statements
coming from a request. Such statements have to be submitted in the body
of a PUT request to the database with a content type of 'text/sql'::

  PUT /db/{db-name}

An example of using this could look like this::

  curl -d "SELECT * FROM {table-name}" -i -H "Content-type: text/sql" http://{host}/db/{db-name}

Note. This is database-specific, so you need to know whether you are
speaking to a PostgreSQL or SQLite-backed webstore.

Command-line usage
------------------

Uploading a spreadsheet::

    curl --data-binary @myfile.csv -i -H "Content-type: text/csv" http://{host}/db/{db-name}?table={table-name}}

Get a filtered JSON representation::

    curl -i -H "Accept: application/json" http://localhost:5000/db/{db-name}/{table-name}?{col}={value}


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

TODO: Specify how to delegate authenatication to user database in some other system.


Plan
====

* DONE. Import existing uml/dataproxy stuff as per Francis' info
* DONE. Get some tests (use existing scraperwiki frontend code)
* DONE. Replace webstore/dataproxy.py with something simpler (probably cyclone based).
* TODO. Implement PUT support
* TODO. Figure out a method to delete individual rows.
* TODO. Find a nice way to address individual rows (sub-resources?)
* TODO. File upload support, maybe with Excel import support.
* TODO. Google Spreadsheet integration.


