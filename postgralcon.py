#!/bin/env python
#-*- coding:utf-8 -*-

__author__ = 'diego.zhu'
__email__ = 'zhuhaiyang55@gmail.com'

import json
import time
import socket
import os
import re
import sys
import getopt
import commands
import urllib2, base64
import psycopg2
import psycopg2.extras
import traceback

debug = False
timestamp = int(time.time())
falconAgentUrl = 'http://127.0.0.1:1988/v1/push'
Step = 60
Metric = 'postgresql'
#send data when error happened
alwaysSend = True
defaultDataWhenFailed = -1
host = '127.0.0.1'
port = '5432'
user = 'postgres'
pswd = 'postgres'
db = 'postgres'
endPoint = socket.gethostname()

class Postgralcon:

    _conn = None
    _curs = None

    monit_keys = [
        'connections',
        'commits',
        'rollbacks',
        'disk_read',
        'buffer_hit',
        'rows_returned',
        'rows_fetched',
        'rows_inserted',
        'rows_updated',
        'rows_deleted',
        'database_size',
        'deadlocks',
        'temp_bytes',
        'temp_files',
        'bgwriter.checkpoints_timed',
        'bgwriter.checkpoints_requested',
        'bgwriter.buffers_checkpoint',
        'bgwriter.buffers_clean',
        'bgwriter.maxwritten_clean',
        'bgwriter.buffers_backend',
        'bgwriter.buffers_alloc',
        'bgwriter.buffersbackendfsync',
        'bgwriter.write_time',
        'bgwriter.sync_time',
        'locks',
        'seq_scans',
        'seqrowsread',
        'index_scans',
        'indexrowsfetched',
        'rowshotupdated',
        'live_rows',
        'dead_rows',
        'indexrowsread',
        'table_size',
        'index_size',
        'total_size',
        'table.count',
        'max_connections',
        'percentusageconnections',
        'heapblocksread',
        'heapblockshit',
        'indexblocksread',
        'indexblockshit',
        'toastblocksread',
        'toastblockshit',
        'toastindexblocks_read',
        'toastindexblocks_hit'
    ]

    def __init__(self):
        self.connected = False

    def tryConnect(self):
        if(self.connected):
            return
        global host,user,pswd,port,db
        self._conn = psycopg2.connect(host=host , user=user, password=pswd , port=port, database=db)
        self._curs = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.connected = True
         
    def __get(self,sql):
        self._curs.execute(sql)
        return self._curs.fetchone()

    def newFalconData(self,key,val,CounterType = 'GAUGE',TAGS = ''):
        global endPoint,Step
        return {
                'Metric': '%s.%s' % (Metric, key),
                'Endpoint': endPoint,
                'Timestamp': timestamp,
                'Step': Step,
                'Value': val,
                'CounterType': CounterType,
                'TAGS': TAGS
            }
    
    def get_connections(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='connections',val=v);
    
    def get_database_size(self):
        global db
        v = self.__get('select pg_database_size(\''+db+'\');')[0]
        return self.newFalconData(key='database_size',val=v);
    
    def get_blocked(self):
        sql = 'SELECT count(*) FROM pg_locks bl'
        sql += ' JOIN pg_stat_activity a ON a.pid = bl.pid '
        sql += ' JOIN pg_locks kl ON kl.transactionid = bl.transactionid AND kl.pid != bl.pid '
        sql += ' JOIN pg_stat_activity ka ON ka.pid = kl.pid WHERE NOT bl.granted;'
        v = self.__get(sql)[0]
        return self.newFalconData(key='blocked',val=v)

def usage():
    print ''
    print 'python postgralcon.py [options]'
    print ''
    print '    -a always send if failed collectting data , kind of falcon nodata , default True'
    print '    -D debug , default False'
    print '    -v default data when failed , default -1'
    print '    -t time-interval , default 60 in second'
    print '    -f falcon-agent-push-url , default http://127.0.0.1:1988/v1/push'
    print '    -m metric , default postgresql'
    print '    -h host:port , default 127.0.0.1:5432'
    print '    -u database user , default postgresql'
    print '    -p database password , default postgresql'
    print '    -e end point , default hostname'
    sys.exit(2)

def main():

    if len(sys.argv[1:]) == 0:
        usage()

    try:
        opts, args = getopt.getopt(sys.argv[1:],"t:f:a:v:d:h:u:p:D:e:H:",['--help'])
    except getopt.GetoptError:
        usage()

    global debug,timestamp,falconAgentUrl,Step,Metric,alwaysSend,defaultDataWhenFailed,host,port,user,pswd,db,endPoint
    for opt, arg in opts:
        if opt in ('-H','--help'):
            usage()
        if opt == '-t':
            Step = arg
        elif opt == '-f':
            falconAgentUrl = arg
        elif opt == '-m':
            Metric = 'postgresql'
        elif opt == '-a':
            alwaysSend = arg.lower() == 'true' and True or False
        elif opt == '-e':
            endPoint = arg
        elif opt == '-v':
            defaultDataWhenFailed = arg
        elif opt == '-h':
            if arg.find(":") == -1:
                print 'illegel param -h %s , should be host:port' % (arg)
                sys.exit(2)
            host = arg.split(':')[0]
            port = arg.split(':')[1]
        elif opt == '-u':
            user = arg
        elif opt == '-p':
            pswd = arg
        elif opt == '-D':
            debug = arg.lower() == 'true' and True or False
    print '[postgralcon]%s@%s:%s/%s %s %s' %(user,host,port,db,falconAgentUrl,Metric)
    if(debug): 
        print "psycopg2 version: "+psycopg2.__version__
    data = []
    monitor = Postgralcon()
    for key in Postgralcon.monit_keys:
        try:
            monitor.tryConnect();
            func_name = "get_"+key
            if hasattr(monitor, func_name):
                func = getattr(monitor, func_name)
                d = func()
                print '%s %s' % (key,d['Value'])
                data.append(d)
            else:
                print'[not supportted yet]'+key
        except Exception, e:
            print '%s %s' % ('[error happened]' , key)
            if(alwaysSend):
                data.append(monitor.newFalconData(key=key,val=defaultDataWhenFailed))
            if(debug):
                print traceback.format_exc()
            continue
    #send data
    if(debug): 
        print json.dumps(data, sort_keys=True,indent=4)
    method = "POST"
    handler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(handler)
    request = urllib2.Request(falconAgentUrl, data=json.dumps(data) )
    request.add_header("Content-Type",'application/json')
    request.get_method = lambda: method
    try:
        connection = opener.open(request)
    except urllib2.HTTPError,e:
        connection = e

    # check. Substitute with appropriate HTTP code.
    if connection.code == 200:
        print connection.read()
    else:
        print '{"err":1,"msg":"%s"}' % connection

main()