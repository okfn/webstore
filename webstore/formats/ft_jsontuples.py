try:
    from json import dumps, loads
except ImportError:
    from simplejson import dumps, loads

from flask import Response, g

def jsontuples_request(request):
    json = loads(request.data)
    keys = json.get('keys', [])
    for row in json.get('data', []):
        yield dict(zip(keys, row))

def _generator(table, callback, keys):
    if callback:
        yield callback + '('
    yield '{"keys": %s, "data": [' % dumps(keys)
    iter = table.__iter__()
    has_next, first = True, True
    while has_next:
        try:
            row = iter.next()
        except StopIteration:
            has_next = False
        if has_next:
            if not first: 
                yield ','
            yield dumps([row[k] for k in keys])
        first = False
    yield ']})' if callback else ']}'

def jsontuples_table(table, keys, headers=None):
    callback = str(g.callback) if g.callback else None
    return Response(_generator(table, callback, keys), mimetype='application/json',
                    direct_passthrough=True, headers=headers)

def jsontuples_message(message, state='error', url=None, code=200):
    keys, values = ['message', 'state'], [message, state]
    if url is not None:
        keys.append('url')
        values.append(url)
    response = Response(dumps({'keys': keys, 'data': [values]}), status=code, 
                        mimetype='application/json')
    if url is not None:
        response.headers['Location'] = url
    return response



