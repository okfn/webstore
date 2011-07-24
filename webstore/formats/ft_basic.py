from flask import render_template, Response

def basic_request(request):
    yield request.form

def _limited(generator, limit=1000):
    for i, row in enumerate(generator):
        yield row
        if i >= limit:
            return 

def basic_table(table, keys):
    return render_template('table.html', keys=keys,
            rows=_limited(table))

def basic_message(message, state='success', url=None, code=200):
    tmpl = render_template('error.html', message=message, url=url)
    response = Response(tmpl, status=code, mimetype='text/html')
    if url is not None:
        response.headers['Location'] = url
    return response
