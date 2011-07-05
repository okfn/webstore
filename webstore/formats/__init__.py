from webstore.formats.ft_csv import csv_request, \
        csv_table, csv_message
from webstore.formats.ft_json import json_request, \
        json_table, json_message
from webstore.formats.ft_basic import basic_request, \
        basic_table, basic_message

MIME_TYPES = {
        'text/html': 'html',
        'application/xhtml+xml': 'html',
        'application/json': 'json',
        'text/javascript': 'json',
        'text/csv': 'csv'
        }

def response_format(request, fmt):
    """ 
    Use HTTP Accept headers (and suffix workarounds) to 
    determine the representation format to be sent to the 
    user.
    """
    best = request.accept_mimetypes \
        .best_match(MIME_TYPES.keys())
    if fmt in MIME_TYPES.values():
        return fmt
    return MIME_TYPES.get(best)

def render_table(request, table, keys, format):
    """ 
    Render a table, which is defined as an iterable of dicts 
    with (at most) the keys in ``keys``.
    """
    format = response_format(request, format)
    if format == 'csv':
        return csv_table(table, keys)
    elif format == 'json':
        return json_table(table, keys)
    else:
        return basic_table(table, keys)

def render_message(request, message, format,
        state='success', code=200, url=None):
    """
    Render a status message, such as an error or a success report.
    If URL is given, it will also be set as a Location: HTTP header.
    """
    format = response_format(request, format)
    if format == 'csv':
        return csv_message(message, state=state, url=url, code=code)
    elif format == 'json':
        return json_message(message, state=state, url=url, code=code)
    else:
        return basic_message(message, state=state, url=url, code=code)

def request_format(request, fmt):
    """ 
    Determine the format of the request content. This is slightly 
    ugly as Flask has excellent request handling built in and we 
    begin to work around it.
    """
    if fmt in MIME_TYPES.values():
        return fmt
    return MIME_TYPES.get(request.content_type, 'html')

def read_request(request, format):
    """
    Handle a request and return a generator which yields all rows 
    in the incoming set.
    """
    format = request_format(request, format)
    if format == 'csv':
        return csv_request(request)
    elif format == 'json':
        return json_request(request)
    else:
        return basic_request(request)


