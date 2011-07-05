from flask import request, url_for

from webstore.core import app
from webstore.formats import render_table, render_message
from webstore.formats import read_request

def _result_proxy(rp):
    keys = rp.keys()
    while True:
        row = rp.fetchone()
        if row is None:
            break
        yield dict(zip(keys, row))

@app.route('/db/<database>.<format>', methods=['GET'])
@app.route('/db/<database>', methods=['GET'])
def index(database, format=None):
    db = app.db_factory.create(database)
    tables = []
    for table in db.engine.table_names():
        url = url_for('read', database=database, table=table)
        tables.append({'name': table,
                       'url': url,
                       'columns': db[table].table.c.keys()})
    return render_table(request, tables, ['name', 'url', 'columns'], format)

@app.route('/db/<database>.<format>', methods=['POST'])
@app.route('/db/<database>', methods=['POST'])
def create(database, format=None):
    if not 'table' in request.args:
        return render_message(request, 'Missing argument: table',
                format, state='error', code=400)
    return create_named(database, request.args.get('table'), 
                        format=format)

@app.route('/db/<database>/<table>.<format>', methods=['POST'])
@app.route('/db/<database>/<table>', methods=['POST'])
def create_named(database, table, format=None):
    db = app.db_factory.create(database)
    if table in db:
        return render_message(request, 'Table already exists: %s' % table,
                format, state='error', code=409, 
                url=url_for('read', database=database, table=table))
    _table = db[table]
    reader = read_request(request, format)
    for row in reader:
        if len(row.keys()):
            _table.add_row(row)
    _table.commit()
    return render_message(request, 'Successfully created: %s' % table,
                format, state='success', code=302,
                url=url_for('read', database=database, table=table))

@app.route('/db/<database>/<table>.<format>', methods=['GET'])
@app.route('/db/<database>/<table>', methods=['GET'])
def read(database, table, format=None):
    db = app.db_factory.create(database)
    if not table in db:
        return render_message(request, 'No such table: %s' % table,
                format, state='error', code=404)
    _table = db[table]
    params = request.args.copy()
    limit = params.pop('_limit', None)
    offset = params.pop('_offset', None)
    try:
        clause = _table.args_to_clause(params)
    except KeyError, ke:
        return render_message(request, 'Invalid filter: %s' % ke,
                format, state='error', code=400)
    stmt = _table.table.select(clause, limit=int(limit) if limit else None, 
                offset=int(offset) if offset else None)
    results = _table.bind.execute(stmt)
    return render_table(request, _result_proxy(results), 
                        results.keys(), format)

@app.route('/db/<database>/<table>.<format>', methods=['PUT'])
@app.route('/db/<database>/<table>', methods=['PUT'])
def update(database, table, format=None):
    pass

@app.route('/db/<database>/<table>.<format>', methods=['DELETE'])
@app.route('/db/<database>/<table>', methods=['DELETE'])
def delete(database, table, format=None):
    db = app.db_factory.create(database)
    if not table in db:
        return render_message(request, 'No such table: %s' % table,
                format, state='error', code=404)
    _table = db[table]
    _table.drop()
    _table.commit()
    return render_message(request, 'Table dropped: %s' % table,
                          format, state='success', code=410)


if __name__ == "__main__":
    app.run()

