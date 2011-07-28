from StringIO import StringIO
from csv import DictReader, writer, DictWriter

from flask import Response
#from ilines import ilines

def csv_request(request):
    reader = DictReader(request.stream)
    for row in reader:
        yield dict([(k, v.decode('utf-8') if v is not None else '') \
                for k,v in row.items()])
        if request.stream.is_exhausted:
            break

def _csv_line(keys, row):
    sio = StringIO()
    csv = writer(sio)
    _row = []
    for key in keys: 
        v = row[key]
        if isinstance(v, unicode):
            v = v.encode('utf-8')
        _row.append(v)
    csv.writerow(_row)
    return sio.getvalue()

def csv_table(table, keys, headers=None):
    def _generator():
        yield _csv_line(keys, dict(zip(keys, keys)))
        for row in table:
            yield _csv_line(keys, row)
    return Response(_generator(), mimetype='text/csv',
                    direct_passthrough=True, headers=headers)

def csv_message(message, state='error', url=None, code=200):
    keys = ['message', 'state', 'url']
    response = Response(mimetype='text/csv', status=code)
    writer = DictWriter(response.stream, keys)
    writer.writerow(dict(zip(keys, keys)))
    writer.writerow({'message': message.encode('utf-8'),
                     'state': state,
                     'url': url.encode('utf-8') if url else ''})
    if url is not None:
        response.headers['Location'] = url
    return response

