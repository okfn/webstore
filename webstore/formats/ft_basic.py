from flask import render_template, Response

def basic_request(request):
    yield request.form

def basic_table(table, keys):
    return render_template('table.tmpl', keys=keys,
            rows=table)

def basic_message(message, state='success', url=None, code=200):
    tmpl = render_template('error.tmpl', message=message, url=url)
    response = Response(tmpl, status=code, mimetype='text/html')
    if url is not None:
        response.headers['Location'] = url
    return response
