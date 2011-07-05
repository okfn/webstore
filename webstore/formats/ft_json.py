from json import dumps

from flask import Response

def json_request(request):
    if not request.json:
        return 
    if not isinstance(request.json, (list, tuple)):
        yield request.json
    for row in request.json:
        yield row

def json_table(table, keys):
    table = [r for r in table]
    return Response(dumps(table), mimetype='application/json')

def json_message(message, state='error', url=None, code=200):
    obj = {'message': message, 'state': state}
    if url is not None:
        obj['url'] = url
    response = Response(dumps(obj), status=code, 
                        mimetype='application/json')
    if url is not None:
        response.headers['Location'] = url
    return response



