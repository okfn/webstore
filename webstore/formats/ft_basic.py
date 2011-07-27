from flask import render_template, Response

from itertools import islice

def basic_request(request):
    yield request.form

def basic_table(table, keys):
    return render_template('table.html', keys=keys,
            rows=islice(table, 1000))

def basic_message(message, state='success', url=None, code=200):
    tmpl = render_template('error.html', message=message, url=url)
    response = Response(tmpl, status=code, mimetype='text/html')
    if url is not None:
        response.headers['Location'] = url
    return response
