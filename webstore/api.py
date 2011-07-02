#!/usr/bin/env python
# coding: utf-8

import webstore
import sys
import cyclone.web
import cyclone.escape
from twisted.python import log
from twisted.internet import reactor
import webstore.datalib

class IndexHandler(cyclone.web.RequestHandler):
    def get(self):
        doc = '''Read from a datastore.

        :param owner: the datastore owner
        :param database: the database name or id
        :param table: the database table
        '''
        owner = self.request.arguments.get('owner', [])
        if not owner:
            self.write('<pre>' + doc + '</pre>')
        else:
            self._handle_request()
    
    def _handle_request(self):
        owner = self.request.arguments.get('owner', [])
        owner = owner[0]
        owner = self.request.arguments.get('owner', [])[0]
        db = self.request.arguments.get('db', [])[0]
        dataauth = 'anyoldthing'
        runID = '1'
        sqlite = webstore.datalib.SQLiteDatabase(self.write, '/tmp', db, dataauth, runID)
        data = self.request.arguments.get('data', [])[0]
        data = cyclone.escape.json_decode(data)
        resp = sqlite.process(data)
        self.write(cyclone.escape.json_encode(resp))


class Application(cyclone.web.Application):
    def __init__(self):
        handlers = [
            (r'/', IndexHandler),
        ]

        settings = {
            'static_path': './static',
        }

        cyclone.web.Application.__init__(self, handlers, **settings)

if __name__ == '__main__':
    log.startLogging(sys.stdout)
    reactor.listenTCP(8888, Application())
    reactor.run()

