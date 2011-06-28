import webstore.client

def test_it_now():
    host = '127.0.0.1'
    port = '9001'
    webstore.client.create(host, port)
    webstore.client.execute('select * from mytable')

