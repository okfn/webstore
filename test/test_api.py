# import webstore.client
import json
import urllib

class Client(object):
    def __init__(self, host, port, owner, database):
        self.host = host
        self.port = port
        self.owner = owner
        self.database = database

    def save(self, table, keys, value):
        data = {
            'maincommand': 'save_sqlite',
            'unique_keys': keys,
            'data': value,
            'swdatatblname': table
            }
        data = json.dumps(data)
        url = 'http://%s:%s/?owner=%s&db=%s&data=%s' % (self.host, self.port, self.owner,
                self.database, data)
        fo = urllib.urlopen(url)
        response = fo.read()
        return json.loads(response)

    def execute(self, query):
        data = {
            'maincommand': 'sqliteexecute',
            'sqlquery': query,
            'data': {},
            'attachlist': []
            }
        data = json.dumps(data)
        url = 'http://%s:%s/?owner=%s&db=%s&data=%s' % (self.host, self.port, self.owner,
                self.database, data)
        fo = urllib.urlopen(url)
        response = fo.read()
        return json.loads(response)
    

def test_it_now():
    host = '127.0.0.1'
    port = '8888'
    client = Client(host, port, 'test', 'test')
    table = 'defaulttbl'
    client.save(table, ['id'], {'id': 1, 'name': 'jones'})
    client.save(table, ['id'], {'id': 'aaa', 'name': 'jones'})
    # print client.show_tables()
    out = client.execute('select * from %s' % table)
    assert len(out.keys()) == 2, out
    print out
    assert out['keys'] == [u'id', u'name'], out


from webstore.datalib import SQLiteDatabase
def test_db():
    output = []
    def echo(out):
        print out
        output.append(out)
    resourcedir = '/tmp'
    db = SQLiteDatabase(echo, resourcedir, 'webstoretest', 'xxxx', '1')
    data = {
            'maincommand': 'save_sqlite',
            'unique_keys': ['id'],
            'data': {'id': 'aaa'},
            'swdatatblname': 'test'
        }
    out = db.process(data)
    assert out['nrecords'] == 1
    request = {
            'maincommand': 'sqliteexecute',
            'sqlquery': 'select * from test',
            'data': {},
            'attachlist': [],
            'streamchunking': False
        }
    out = db.process(request)
    print out
    print output
    assert out['keys'] == ['id']

