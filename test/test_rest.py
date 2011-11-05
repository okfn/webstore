#coding: utf-8
import os
import shutil
import json
from StringIO import StringIO
from csv import DictReader
import webstore.web as ws
import unittest
import tempfile

JSON = 'application/json'
JSONT = 'application/json+tuples'
CSV = 'text/csv'

CSV_FIXTURE = """date,temperature,place
2011-01-01,1,Galway
2011-01-02,-1,Galway
2011-01-03,0,Galway
2011-01-01,6,Berkeley
2011-01-02,8,Berkeley
2011-01-03,5,Berkeley"""

JSON_FIXTURE = [{'row1': 'rowvalue1', 'foo': 'bar'},
                {'row1': 'value2', 'foo': 'schnasel'}]

JSON_ALTER_FIXTURE = [{'row1': 'rowvalue1', 'foo': 'bar'},
                      {'row1': 'value2', 'foo': 'schnasel', 'row3': 'master'}]

JSON_TYPED_FIXTURE = [{'int_col': 5, 'str_col': 'foo', 
                       'float_col': 6.55, 'null_col': None},
                      {'int_col': 2, 'str_col': 'bar',
                       'float_col': 2.1, 'null_col': 5}]

JSONTUPLES_FIXTURE = {"keys": ["foo", "bar"], 
                      "data": [["fval1", "bval1"],
                               ["fval2", "bval2"]]}

class WebstoreTestCase(unittest.TestCase):

    def setUp(self):
        ws.app.config['SQLITE_DIR'] = tempfile.mkdtemp()
        ws.app.config['TESTING'] = True
        ws.app.config['AUTHORIZATION']['world'] = \
                ['read', 'write', 'delete']
        self.app = ws.app.test_client()
        self.make_fixtures()

    def tearDown(self):
        shutil.rmtree(ws.app.config['SQLITE_DIR'])

    def make_fixtures(self):
        self.app.post('/hugo/fixtures?table=json',
                content_type=JSON,
                data=json.dumps(JSON_FIXTURE))
        self.app.post('/hugo/fixtures?table=csv',
                content_type=CSV,
                data=CSV_FIXTURE)
    
    def test_user_databases(self):
        response = self.app.get('/xxx', headers={'Accept': JSON})
        body = json.loads(response.data)
        assert response.status.startswith("404"), response.status
        response = self.app.get('/hugo', headers={'Accept': JSON})
        body = json.loads(response.data)
        assert len(body['data'])==1
        assert body['data'][0]['name']=='fixtures'

    def test_no_tables(self):
        response = self.app.get('/hugo/no_tables', headers={'Accept': JSON})
        assert json.loads(response.data) == {"count": None, "fields": [{"name": "name"}, {"name": "url"}, {"name": "columns"}], "data": []} 
    
    def test_invalid_db_name(self):
        response = self.app.get('/hugo/no such db', headers={'Accept': JSON})
        assert response.status.startswith("400"), response.status
    
    def test_valid_db_name(self):
        response = self.app.get('/hugo/no-such-db', headers={'Accept': JSON})
        assert response.status.startswith("200"), response.status
        response = self.app.get('/hu-go/no-such-db', headers={'Accept': JSON})
        assert response.status.startswith("200"), response.status
    
    def test_invalid_table_name(self):
        response = self.app.get('/hugo/no such db/foo', headers={'Accept': JSON})
        assert response.status.startswith("400"), response.status

    def test_create_json_table(self):
        response = self.app.post('/hugo/create_json_table?table=foo',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(JSON_FIXTURE))
        body = json.loads(response.data)
        assert 'Successfully' in body['message'], body
        assert 'success' == body['state'], body
        assert '/hugo/create_json_table/foo' == body['url'], body
    
    def test_create_json_tuples_table(self):
        response = self.app.post('/hugo/create_jsont_table?table=foo',
                headers={'Accept': JSON}, content_type=JSONT,
                data=json.dumps(JSONTUPLES_FIXTURE))
        body = json.loads(response.data)
        assert 'Successfully' in body['message'], body
        assert 'success' == body['state'], body
        assert '/hugo/create_jsont_table/foo' == body['url'], body
        response = self.app.get('/hugo/create_jsont_table/foo',
                headers={'Accept': JSON})
        
        body = json.loads(response.data)
        assert len(body['data'])==2,body
        assert body['data'][0]['foo']=='fval1',body
        assert body['data'][0]['bar']=='bval1',body
        assert body['data'][1]['foo']=='fval2',body
    
    def test_create_typed_json_table(self):
        response = self.app.post('/hugo/foobar?table=typed',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(JSON_TYPED_FIXTURE))
        body = json.loads(response.data)
        assert 'Successfully' in body['message'], body
        response = self.app.get('/hugo/foobar/typed', 
                headers={'Accept': JSON})
        data = json.loads(response.data)
        in_row = JSON_TYPED_FIXTURE[0]
        out_row = data['data'][0]
        assert in_row['float_col']-out_row['float_col']<0.1, out_row
        assert in_row['int_col']==out_row['int_col'], out_row
        assert type(data['data'][1]['null_col'])==unicode, data[1]

    def test_create_csv_table(self):
        response = self.app.post('/hugo/create_csv_table?table=foo',
                headers={'Accept': CSV}, content_type=CSV,
                data=CSV_FIXTURE)
        assert 'message,state,url' in response.data, response.data
        assert 'Successfully' in response.data, response.data
        assert '/hugo/create_csv_table/foo' in response.data, response.data
    
    def test_index_with_tables(self):
        response = self.app.get('/hugo/fixtures', headers={'Accept': JSON})
        data = json.loads(response.data)
        assert len(data['data']) == 2, data
    
    def test_index_db_download(self):
        response = self.app.get('/hugo/ghost', 
                headers={'Accept': 'application/x-sqlite3'})
        assert response.status.startswith("404"), response.status
        response = self.app.get('/hugo/fixtures', 
                headers={'Accept': 'application/x-sqlite3'})
        assert response.data.startswith("SQLite format 3"), response.data

    def test_upsert_table(self):
        response = self.app.post('/hugo/fixtures/json',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(JSON_FIXTURE))
        body = json.loads(response.data)
        assert response.status == "201 CREATED", response.status
        assert body['state'] == 'success', body

    def test_upsert_alter_table(self):
        response = self.app.post('/hugo/fixtures/json_more',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(JSON_ALTER_FIXTURE))
        body = json.loads(response.data)
        assert response.status == "201 CREATED", response.status
        assert body['state'] == 'success', body
    
    def test_create_invalid_database_name(self):
        response = self.app.post('/hugo/_fooschnasel/foo',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(JSON_FIXTURE))
        assert response.status.startswith("400"), response.status
    
    def test_create_invalid_column_name(self):
        data = [{'invalid column': 'not good', u'_valdätion': 'priceless'}]
        response = self.app.post('/hugo/fooschnasel/foo',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(data))
        assert response.status.startswith("400"), response.status

    def test_create_invalid_table_name(self):
        response = self.app.post('/hugo/ooschnasel/_foo',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(JSON_FIXTURE))
        assert response.status.startswith("400"), response.status

    def test_does_not_exist(self):
        response = self.app.get('/hugo/fixtures/not_there')
        assert response.status.startswith("404"), response.status

    def test_read_json_representation(self):
        response = self.app.get('/hugo/fixtures/json',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert len(body['data']) == len(JSON_FIXTURE), body

    def test_read_json_representation_with_count(self):
        response = self.app.get('/hugo/fixtures/json?_count=1',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert len(body['data']) == len(JSON_FIXTURE), body
        assert body['count'] == len(JSON_FIXTURE), len(JSON_FIXTURE)
    
    def test_read_json_tuples_representation(self):
        response = self.app.get('/hugo/fixtures/json',
            headers={'Accept': JSONT})
        body = json.loads(response.data)
        assert 'keys' in body, body
        assert 'data' in body, body
        keys = body.get('keys')
        values = body.get('data')
        assert len(keys) == 3, keys
        assert len(keys) == len(values[0]), values
    
    def test_read_json_representation_with_limit_and_offset(self):
        response = self.app.get('/hugo/fixtures/csv?_limit=2&_offset=2',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert body['data'][0].get('place')=='Galway', body
        assert body['data'][1].get('place')=='Berkeley', body

    def test_read_csv_representation(self):
        response = self.app.get('/hugo/fixtures/csv',
            headers={'Accept': CSV})
        reader = DictReader(StringIO(response.data))
        flds = ['__id__', 'date', 'place', 'temperature']
        assert reader.fieldnames == flds, reader.fieldnames
        assert len(list(reader))==6
    
    def test_read_json_representation_invalid_limit(self):
        response = self.app.get('/hugo/fixtures/csv?_limit=BANANA',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert response.status.startswith("400"), response.status
        assert 'BANANA' in body.get('message'), body
    
    def test_read_json_representation_invalid_sort(self):
        response = self.app.get('/hugo/fixtures/csv?_sort=theotherway',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert response.status.startswith("400"), response.status
        assert 'Invalid sorting format' in body.get('message'), body
    
    def test_read_json_representation_sort(self):
        response = self.app.get('/hugo/fixtures/csv?_sort=desc:temperature',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert body['data'][0]['temperature'] == '8', body
        assert body['data'][0]['place'] == 'Berkeley', body
    
    # FIXME: Headers do not appear in testing harness
    #def test_read_json_representation_count(self):
    #    response = self.app.get('/hugo/fixtures/csv',
    #        headers={'Accept': JSON})
    #    print response.get_wsgi_response()
    #    print dir(response)
    #    assert response.headers['X-Count']==6, response.headers
    
    def test_read_json_schema(self):
        response = self.app.get('/hugo/fixtures/csv/schema',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert len(body['data']) == 4, body
        for col_desc in body['data']:
            assert 'name' in col_desc, col_desc
            assert len(col_desc['name']), col_desc
            assert 'type' in col_desc, col_desc
            assert len(col_desc['type']), col_desc
            assert 'values_url' in col_desc, col_desc
            assert len(col_desc['values_url']), col_desc
            assert col_desc['values_url'].startswith('/hugo/fixtures/csv/distinct/'), col_desc

    def test_read_typed_json_schema(self):
        response = self.app.post('/hugo/foobar?table=typed',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(JSON_TYPED_FIXTURE))
        response = self.app.get('/hugo/foobar/typed/schema',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert len(body['data'])==5, body
        for col in body['data']:
            if col['name'] == 'float_col':
                assert col['type']=='float', col
            if col['name'] == 'int_col':
                assert col['type']=='integer', col

        invalid = [{'float_col': 'hallo'}]
        response = self.app.put('/hugo/foobar/typed',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(invalid))
        assert response.status.startswith("400"), response.status
        data = json.loads(response.data)
        assert 'float' in data['message'], data

    def test_put_additional_row(self):
        update = [{'place': 'Honolulu', 'climate': 'mild'}]
        response = self.app.put('/hugo/fixtures/csv',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(update))
        assert response.status.startswith("201"), response.status
        response = self.app.get('/hugo/fixtures/csv?climate=mild',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert body['data'][0]['place'] == 'Honolulu', body

    def test_put_invalid_column_name(self):
        data = [{'invalid column': 'not good', u'_valdätion': 'priceless'}]
        response = self.app.put('/hugo/fixtures/csv',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(data))
        assert response.status.startswith("400"), response.status

    def test_put_additional_row_as_json_dict(self):
        update = {'place': 'Honolulu', 'climate': 'mild'}
        response = self.app.put('/hugo/fixtures/csv',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(update))
        assert response.status.startswith("201"), response.status
        response = self.app.get('/hugo/fixtures/csv?climate=mild',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert body['data'][0]['place'] == 'Honolulu', body

    def test_put_additional_row_with_unique_selector(self):
        update = [{'place': 'Berkeley', 'country': 'United States'}]
        response = self.app.put('/hugo/fixtures/csv?unique=place',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(update))
        assert response.status.startswith("201"), response.status
        response = self.app.get('/hugo/fixtures/csv?place=Berkeley',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert body['data'][0]['country'] == 'United States', body

    def test_put_sql_request(self):
        query = 'SELECT * FROM "csv"'
        response = self.app.put('/hugo/fixtures',
                headers={'Accept': JSON}, content_type='text/sql',
                data=query)
        body = json.loads(response.data)
        assert len(body['data']) == 6, body
        assert body['data'][0]['place'] is not None, body
    
    def test_put_sql_request_with_params(self):
        query = {'query': 'SELECT * FROM "csv" WHERE place = ?',
                 'params': ['Galway']}
        response = self.app.put('/hugo/fixtures',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(query))
        body = json.loads(response.data)
        assert len(body['data']) == 3, body
        assert body['data'][0]['place']=='Galway', body

        query = {'query': 'SELECT * FROM "csv" WHERE place = :foo',
                 'params': {'foo': 'Galway'}}
        response = self.app.put('/hugo/fixtures',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(query))
        body = json.loads(response.data)
        assert len(body['data']) == 3, body
        assert body['data'][0]['place']=='Galway', body
    
    def test_put_sql_attach_database(self):
        query = {'query': 'SELECT * FROM foo."csv"',
                 'attach': [{'user': 'hugo', 
                             'database': 'fixtures', 
                             'alias': 'foo'}]}
        response = self.app.put('/hugo/mixtures',
                headers={'Accept': JSON}, content_type=JSON,
                data=json.dumps(query))
        body = json.loads(response.data)
        assert len(body['data']) == 6, body
        assert body['data'][0]['place']=='Galway', body

    def test_read_json_single_row(self):
        response = self.app.get('/hugo/fixtures/json/row/0',
            headers={'Accept': JSON})
        assert response.status.startswith("400"), response.status

        response = self.app.get('/hugo/fixtures/csv/row/3',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert response.status.startswith("200"), response.status
        assert body['data'][0]['place'] == 'Galway', body
        assert body['data'][0]['temperature'] == '0', body

    def test_read_json_distinct_column(self):
        response = self.app.get('/hugo/fixtures/json/distinct/not_a_column',
            headers={'Accept': JSON})
        assert response.status.startswith("404"), response.status
        response = self.app.get('/hugo/fixtures/csv/distinct/place',
            headers={'Accept': JSON})
        body = json.loads(response.data)
        assert response.status.startswith("200"), response.status
        assert len(body['data'])==2, body


CKAN_DB_FIXTURE = os.path.join(os.path.dirname(__file__), 'ckan.db')
APIKEY = 'test-api-key'
class AuthAndAuthzTestCase(unittest.TestCase):
    def setUp(self):
        ws.app.config['SQLITE_DIR'] = tempfile.mkdtemp()
        ws.app.config['TESTING'] = True
        ws.app.config['AUTHORIZATION']['world'] = \
                ['read', 'write', 'delete']
        self.app = ws.app.test_client()
        self.make_fixtures()

    def tearDown(self):
        shutil.rmtree(ws.app.config['SQLITE_DIR'])

    def make_fixtures(self):
        self.app.post('/hugo/fixtures?table=json',
                content_type=JSON,
                data=json.dumps(JSON_FIXTURE))
        self.app.post('/hugo/fixtures?table=csv',
                content_type=CSV,
                data=CSV_FIXTURE)

    def test_database_index_authorization(self):
        # kill all permissions:
        ws.app.config['AUTHORIZATION']['world'] = []
        response = self.app.get('/hugo/fixtures', 
                    headers={'Accept': JSON})
        assert response.status.startswith("403"), response.status

        ws.app.config['AUTHORIZATION']['user'] = []
        response = self.app.get('/hugo/fixtures', 
                    headers={'Accept': JSON}, 
                    environ_base={'REMOTE_USER': 'bert'})
        assert response.status.startswith("403"), response.status

        response = self.app.get('/hugo/fixtures', 
                    headers={'Accept': JSON}, 
                    environ_base={'REMOTE_USER': 'hugo'})
        assert not response.status.startswith("403"), response.status

    def test_database_create_authorization(self):
        record = [{'place': 'Honolulu', 'climate': 'mild'}]
        ws.app.config['AUTHORIZATION']['world'] = []
        response = self.app.post('/hugo/fixtures/authz', 
                    headers={'Accept': JSON}, content_type=JSON, 
                    data=json.dumps(record))
        assert response.status.startswith("403"), response.status

        ws.app.config['AUTHORIZATION']['world'] = ['write']
        response = self.app.post('/hugo/fixtures/authz', 
                    headers={'Accept': JSON}, content_type=JSON, 
                    data=json.dumps(record))
        assert response.status.startswith("201"), response.status

    def test_login_challenge(self):
        # kill all permissions:
        response = self.app.get('/login', headers={'Accept': JSON})
        assert response.status.startswith("401"), response.status

    def test_login_http_basic_authorization(self):
        ws.app.config['AUTH_FUNCTION'] = 'always_login'
        auth = 'Basic ' + 'hugo:hungry'.encode('base64')
        response = self.app.get('/hugo/fixtures', headers={'Accept': JSON,
            'Authorization': auth})
        assert response.status.startswith("200"), response.status
        
        ws.app.config['AUTH_FUNCTION'] = 'never_login'
        auth = 'Basic ' + 'hugo:hungry'.encode('base64')
        response = self.app.get('/hugo/fixtures', headers={'Accept': JSON,
            'Authorization': auth})
        assert response.status.startswith("401"), response.status
    
    def test_login_http_basic_authorization_with_ckan_db(self):
        ws.app.config['AUTH_FUNCTION'] = 'ckan'
        ws.app.config['CKAN_DB_URI'] = 'sqlite:///' + CKAN_DB_FIXTURE
        auth = APIKEY
        response = self.app.get('/test/fixtures', headers={'Accept': JSON,
            'Authorization': auth})
        assert response.status.startswith("200"), response.status
        
        auth = 'bad-api-key'
        response = self.app.get('/test/fixtures', headers={'Accept': JSON,
            'Authorization': auth})
        assert response.status.startswith("401"), response.status


if __name__ == '__main__':
    unittest.main()

