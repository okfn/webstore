##!/bin/sh -
"exec" "python" "-O" "$0" "$@"

import BaseHTTPServer
import SocketServer
import urllib
import urlparse
import cgi
import signal
import os
import sys
import time
import ConfigParser
import datetime
import optparse
import grp
import pwd
import datalib
import socket

import logging
import logging.config
try:
    import cloghandler
except:
    pass

try:
    import json
except:
    import simplejson as json

# note: there is a symlink from /var/www/scraperwiki to the scraperwiki directory
# which allows us to get away with being crap with the paths

configfile = '/var/www/scraperwiki/uml/uml.cfg'
config = ConfigParser.ConfigParser()
config.readfp(open(configfile))

class ProxyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    __base         = BaseHTTPServer.BaseHTTPRequestHandler
    __base_handle  = __base.handle

    server_version = "DataProxy/ScraperWiki_0.0.1"
    rbufsize       = 0

    def ident(self, uml, port):
        runID      = None
        short_name = None

        host      = config.get(uml, 'host')
        via       = config.get(uml, 'via' )
        rem       = self.connection.getpeername()
        loc       = self.connection.getsockname()
        lident     = urllib.urlopen ('http://%s:%s/Ident?%s:%s' % (host, via, port, loc[1])).read()   

                # should be using cgi.parse_qs(query) technology here
        for line in lident.split('\n'):
            if line:
                key, value = line.split('=')
                if key == 'runid':
                    runID = value
                elif key == 'scrapername':
                    short_name = value

        return runID, short_name

    def process(self, db, request):
        self.logger.debug(str(("request", request))[:100])

        res = db.process(request)
        
        sres = json.dumps(res)
        self.logger.debug(sres[:200])
        self.connection.sendall(sres+'\n')
            


        # this morphs into the long running two-way connection
    def do_GET (self) :
        try:
            self.logger = logging.getLogger('dataproxy')

            (scm, netloc, path, params, query, fragment) = urlparse.urlparse(self.path, 'http')
            params = dict(cgi.parse_qsl(query))

            firstmessage = {"status":"good"}
            if 'short_name' in params:
                if self.connection.getpeername()[0] != config.get('dataproxy', 'secure') :
                    firstmessage = {"error":"short_name only accepted from secure hosts"}
                else:
                    short_name = params.get('short_name', '')
                    runID = 'fromfrontend.%s.%s' % (short_name, time.time()) 
                    dataauth = "fromfrontend"
            
            else:
                # TODO: reinstate this once understand what is going on
                # runID, short_name = self.ident(params['uml'], params['port'])
                runID = '1'
                short_name = ''
                if not runID:
                    firstmessage = {"error":"ident failed no runID"}
                elif runID[:8] == "draft|||" and short_name:
                    dataauth = "draft"
                else:
                    dataauth = "writable"
            
            if path == '' or path is None :
                path = '/'

            if scm not in ['http', 'https'] or fragment:
                firstmessage = {"error":"Malformed URL %s" % self.path}

            # consolidate sending back to trap socket errors
            try:
                self.connection.sendall(json.dumps(firstmessage)+'\n')
            except socket.error:
                self.logger.warning("connection to dataproxy socket.error: "+str(firstmessage))
            if "error" in firstmessage:
                self.logger.warning("connection to dataproxy refused error: "+str(firstmessage["error"]))
                self.connection.close()
                return
            
            self.logger.debug("connection made to dataproxy for %s %s - %s" % (dataauth, short_name, runID))
            db = datalib.SQLiteDatabase(self, config.get('dataproxy', 'resourcedir'), short_name, dataauth, runID)

                    # enter the loop that now waits for single requests (delimited by \n) 
                    # and sends back responses through a socket
                    # all with json objects -- until the connection is terminated
            sbuffer = [ ]

            while True:
                try:
                    srec = self.connection.recv(255)
                except socket.error:
                    self.logger.warning("connection to from uml recv error: "+str([runID, short_name]))
                    break
                
                ssrec = srec.split("\n")  # multiple strings if a "\n" exists
                sbuffer.append(ssrec.pop(0))
                while ssrec:
                    line = "".join(sbuffer)
                    if line:
                        request = json.loads(line) 
                        try:
                            self.process(db, request)
                        except socket.error:
                            self.logger.warning("connection sending to uml socket.error: "+str([runID, short_name]))
                            srec = ""  # break out of loop
                    sbuffer = [ ssrec.pop(0) ]  # next one in
                if not srec:
                    break
            self.logger.debug("ending connection %s - %s" % (short_name, runID))
            self.connection.close()
        except:
            self.logger.error("do_GET uncaught exception: %s %s %s" % sys.exc_info())


    do_HEAD   = do_GET
    do_POST   = do_GET
    do_PUT    = do_GET
    do_DELETE = do_GET


class ProxyHTTPServer(SocketServer.ForkingMixIn, BaseHTTPServer.HTTPServer):
    pass

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("--setuid", action="store_true")
    parser.add_option("--pidfile")
    parser.add_option("--logfile")
    parser.add_option("--toaddrs", default="")
    poptions, pargs = parser.parse_args()

    # daemon mode
    if os.fork() == 0 :
        os.setsid()
        sys.stdin = open('/dev/null')
        if os.fork() == 0 :
            ppid = os.getppid()
            while ppid != 1 :
                time.sleep(1)
                ppid = os.getppid()
        else :
            os._exit (0)
    else :
        os.wait()
        sys.exit (1)

    pf = open(poptions.pidfile, 'w')
    pf.write('%d\n' % os.getpid())
    pf.close()
        
    if poptions.setuid:
        gid = grp.getgrnam("nogroup").gr_gid
        os.setregid(gid, gid)
        uid = pwd.getpwnam("nobody").pw_uid
        os.setreuid(uid, uid)

    logging.config.fileConfig(configfile)

    port = config.getint('dataproxy', 'port')
    ProxyHandler.protocol_version = "HTTP/1.0"
    httpd = ProxyHTTPServer(('', port), ProxyHandler)
    httpd.max_children = 160
    sa = httpd.socket.getsockname()
    logger = logging.getLogger('dataproxy')
    logger.info(str(("Serving HTTP on", sa[0], "port", sa[1], "...")))
    httpd.serve_forever()

