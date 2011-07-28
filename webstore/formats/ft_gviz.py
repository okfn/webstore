from itertools import islice

from flask import Response

def gviz_table(table, keys, headers=None):
    try:
        import gviz_api
        description = dict([(k, ()) for k in keys])
        data_table = gviz_api.DataTable(description)
        data_table.LoadData(list(islice(table, 10000)))
        return Response(data_table.ToJSon(), mimetype='text/plain',
                headers=headers)
    except ImportError:
        return Response('GViz Exporter not installed.', status=501, mimetype='text/plain')

