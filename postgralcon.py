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

class Postgralcon:
    _host = '127.0.0.1'
    _port = '5432'
    _user = 'postgres'
    _pass = 'postgres'
    _db = 'postgres'
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
        self._conn = psycopg2.connect(host=self._host , user=self._user, password=self._pass , port=self._port, database=self._db)
        self._curs = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.connected = True
         
    def __get(self,sql):
        self._curs.execute(sql)
        return self._curs.fetchone()

    def newFalconData(self,key,val,CounterType = 'GAUGE',TAGS = None):
        return {
                'Metric': '%s.%s' % (Metric, key),
                'Endpoint': socket.gethostname(),
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
        v = self.__get('select pg_database_size(\''+self._db+'\');')[0]
        return self.newFalconData(key='database_size',val=v);
    
    def get_blocked(self):
        sql = 'SELECT count(*) FROM pg_locks bl'
        sql += ' JOIN pg_stat_activity a ON a.pid = bl.pid '
        sql += ' JOIN pg_locks kl ON kl.transactionid = bl.transactionid AND kl.pid != bl.pid '
        sql += ' JOIN pg_stat_activity ka ON ka.pid = kl.pid WHERE NOT bl.granted;'
        v = self.__get(sql)[0]
        return self.newFalconData(key='blocked',val=v)

if(debug): 
    print "psycopg2 version: "+psycopg2.__version__

def main():
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

if __name__ == '__main__':
    proc = commands.getoutput(' ps -ef|grep %s|grep -v grep|wc -l ' % os.path.basename(sys.argv[0]))
    if int(proc) < 5:
        main()
