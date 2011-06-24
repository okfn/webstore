import re
import datetime
import scraperwiki

class SqliteError(Exception):  pass
class NoSuchTableSqliteError(SqliteError):  pass

attachlist = [ ]

def strunc(v, t):
    if not t or len(v) < t:
        return v
    return "%s..." % v[:t]

def strencode_trunc(v, t):
    if type(v) == str:
        v = v.decode('utf-8')
    else:
        v = unicode(v)

    try:
        return strunc(v, t).encode('utf-8')
    except:
        return "---"


def ifsencode_trunc(v, t):
    if type(v) in [int, float]:
        return v
    return strencode_trunc(v, t)


def execute(sqlquery, data=None, verbose=1):
    global attachlist
    result = scraperwiki.datastore.request({"maincommand":'sqliteexecute', "sqlquery":sqlquery, "data":data, "attachlist":attachlist})
    if "error" in result:
        raise databaseexception(result)
    if "status" not in result and "keys" not in result:
        raise Exception("possible signal timeout: "+str(result))
    
    # list type for second field in message dump
    if verbose:
        if data == None:
            ldata = [ ]
        elif type(data) in [tuple, list]:
            ldata = [ ifsencode_trunc(v, 50)  for v in data ]
        elif type(data) == dict:
            ldata = [ ifsencode_trunc(v, 50)  for v in data.values() ]
        else:
            ldata = [ str(data) ]
        scraperwiki.dumpMessage({'message_type':'sqlitecall', 'command':'sqliteexecute', "val1":sqlquery, "lval2":ldata})
    
    return result
    


def databaseexception(errmap):
    mess = errmap["error"]
    for k, v in errmap.items():
        if k != "error":
            mess = "%s; %s:%s" % (mess, k, v)
    
    if re.match('sqlite3.Error: no such table:', mess):
        return NoSuchTableSqliteError(mess)
    return SqliteError(mess)
        



def save(unique_keys, data, table_name="swdata", verbose=2, date=None):
    if unique_keys != None and type(unique_keys) not in [ list, tuple ]:
        return { "error":'unique_keys must a list or tuple', "unique_keys_type":str(type(unique_keys)) }

    def convdata(unique_keys, scraper_data):
        if unique_keys:
            for key in unique_keys:
                if key not in scraper_data:
                    return { "error":'unique_keys must be a subset of data', "bad_key":key }
                if scraper_data[key] == None:
                    return { "error":'unique_key value should not be None', "bad_key":key }
        jdata = { }
        for key, value in scraper_data.items():
            if not key:
                return { "error": 'key must not be blank', "bad_key":key }
            if type(key) not in [unicode, str]:
                return { "error":'key must be string type', "bad_key":key }
            if not re.match("[a-zA-Z0-9_\- ]+$", key):
                return { "error":'key must be simple text', "bad_key":key }
            
            if type(value) in [datetime.datetime, datetime.date]:
                value = value.isoformat()
            elif value == None:
                pass
            elif isinstance(value, SqliteError):
                return {"error": str(value)}
            elif type(value) == str:
                try:
                    value = value.decode("utf-8")
                except:
                    return {"error": "Binary strings must be utf-8 encoded"}
            elif type(value) not in [int, bool, float, unicode, str]:
                value = unicode(value)
            jdata[key] = value
        return jdata
            

    if type(data) == dict:
        rjdata = convdata(unique_keys, data)
        if rjdata.get("error"):
            raise databaseexception(rjdata)
        if date:
            rjdata["date"] = date
    else:
        rjdata = [ ]
        for ldata in data:
            ljdata = convdata(unique_keys, ldata)
            if ljdata.get("error"):
                raise databaseexception(ljdata)
            rjdata.append(ljdata)
    result = scraperwiki.datastore.request({"maincommand":'save_sqlite', "unique_keys":unique_keys, "data":rjdata, "swdatatblname":table_name})

    if "error" in result:
        raise databaseexception(result)

    if verbose >= 2:
        pdata = {}
        if type(data) == dict:
            for key, value in data.items():
                pdata[strencode_trunc(key, 50)] = strencode_trunc(value, 50)
        elif data:
            for key, value in data[0].items():
                pdata[strencode_trunc(key, 50)] = strencode_trunc(value, 50)
            pdata["number_records"] = "Number Records: %d" % len(data)
        scraperwiki.dumpMessage({'message_type':'data', 'content': pdata})
    return result


def attach(name, asname=None, verbose=1):
    global attachlist
    attachlist.append({"name":name, "asname":asname})
    result = scraperwiki.datastore.request({"maincommand":'sqlitecommand', "command":"attach", "name":name, "asname":asname})
    if "error" in result:
        raise databaseexception(result)
    if "status" not in result:
        raise Exception("possible signal timeout: "+str(result))
        scraperwiki.dumpMessage({'message_type':'data', 'content': pdata})
    return result


def commit(verbose=1):
    result = scraperwiki.datastore.request({"maincommand":'sqlitecommand', "command":"commit"})
    if "error" in result:
        raise databaseexception(result)
    if "status" not in result:
        raise Exception("possible signal timeout: "+str(result))
        scraperwiki.dumpMessage({'message_type':'data', 'content': pdata})
    return result    
    


def select(sqlquery, data=None, verbose=1):
    if data is not None and "?" in sqlquery and type(data) not in [list, tuple]:
        data = [data]
    sqlquery = "select %s" % sqlquery   # maybe check if select or another command is there already?
    result = execute(sqlquery, data, verbose=verbose)
    return [ dict(zip(result["keys"], d))  for d in result["data"] ]


def show_tables(dbname=""):
    name = "sqlite_master"
    if dbname:
        name = "`%s`.%s" % (dbname, name)
    result = execute("select tbl_name, sql from %s where type='table'" % name)
    return dict(result["data"])


def table_info(name):
    sname = name.split(".")
    if len(sname) == 2:
        result = execute("PRAGMA %s.table_info(`%s`)" % tuple(sname))
    else:
        result = execute("PRAGMA table_info(`%s`)" % name)
    return [ dict(zip(result["keys"], d))  for d in result["data"] ]


            # also needs to handle the types better (could save json and datetime objects handily
def save_var(name, value, verbose=2):
    data = {"name":name, "value_blob":value, "type":type(value).__name__}
    save(unique_keys=["name"], data=data, table_name="swvariables", verbose=verbose)

def get_var(name, default=None, verbose=2):
    try:
        result = execute("select value_blob, type from swvariables where name=?", (name,), verbose)
    except NoSuchTableSqliteError, e:
        return default
    data = result.get("data")
    if not data:
        return default
    return data[0][0]




