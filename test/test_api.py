import webstore.client
import json

def test_it_now():
    host = '127.0.0.1'
    port = '9034'
    webstore.client.create(host, port)
    webstore.client.save(['id'], {'id': 1, 'name': 'jones'})
    webstore.client.save(['id'], {'id': 'aaa', 'name': 'jones'})
    print webstore.client.show_tables()
    out = webstore.client.execute('select * from swdata')
    assert len(out.keys()) == 2, data
    assert out['keys'] == [u'id', u'name']

