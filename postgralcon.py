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
step = 60
metric = 'postgresql'
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
        global endPoint,step,metric
        return {
                'Metric': '%s.%s' % (metric, key),
                'Endpoint': endPoint,
                'Timestamp': timestamp,
                'Step': step,
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

    def get_commits(self):
        global db
        v = self.__get('select xact_commit from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='commits', val=v)

    def get_rollbacks(self):
        global db
        v = self.__get('select xact_rollback from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='rollbacks', val=v)

    def get_disk_read(self):
        global db
        v = self.__get('select blks_read from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='disk_read', val=v)

    def get_buffer_hit(self):
        global db
        v = self.__get('select blks_hit from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='buffer_hit', val=v)

    def get_rows_returned(self):
        global db
        v = self.__get('select tup_returned from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='rows_returned', val=v)

    def get_rows_fetched(self):
        global db
        v = self.__get('select tup_fetched from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='rows_fetched', val=v)

    def get_rows_inserted(self):
        global db
        v = self.__get('select tup_inserted from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='rows_inserted', val=v)

    def get_rows_updated(self):
        global db
        v = self.__get('select tup_updated from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='rows_updated', val=v)

    def get_rows_deleted(self):
        global db
        v = self.__get('select tup_deleted from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='rows_deleted', val=v)

    def get_temp_bytes(self):
        global db
        v = self.__get('select temp_bytes from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='temp_bytes', val=v)

    def get_temp_files(self):
        global db
        v = self.__get('select temp_files from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='temp_files', val=v)



    def get_deadlocks(self):
        global db
        v = self.__get('select deadlocks from pg_stat_database where datname=\'' + db + '\';')[0]
        return self.newFalconData(key='deadlocks', val=v)

    def get_bgwriter_checkpoints_timed(self):
        v = self.__get('select checkpoints_timed from pg_stat_bgwriter;')[0]
        return self.newFalconData(key='bgwriter_checkpoints_timed', val=v)

    def get_bgwriter_checkpoints_requested(self):
        v = self.__get('select checkpoints_req from pg_stat_bgwriter;')[0]
        return self.newFalconData(key='bgwriter_checkpoints_requested', val=v)

    def get_bgwriter_buffers_checkpoint(self):
        v = self.__get('select buffers_checkpoint from pg_stat_bgwriter;')[0]
        return self.newFalconData(key='bgwriter_buffers_checkpoint', val=v)

    def get_bgwriter_buffers_clean(self):
        v = self.__get('select buffers_clean from pg_stat_bgwriter;')[0]
        return self.newFalconData(key='bgwriter_buffers_clean', val=v)

    def get_bgwriter_maxwritten_clean(self):
        v = self.__get('select maxwritten_clean from pg_stat_bgwriter;')[0]
        return self.newFalconData(key='bgwriter_maxwritten_clean', val=v)

    def get_bgwriter_buffers_backend(self):
        v = self.__get('select buffers_backend from pg_stat_bgwriter;')[0]
        return self.newFalconData(key='bgwriter_buffers_backend', val=v)

    def get_bgwriter_buffers_alloc(self):
        v = self.__get('select buffers_alloc from pg_stat_bgwriter;')[0]
        return self.newFalconData(key='bgwriter_buffers_alloc', val=v)

    def get_bgwriter_buffersbackendfsync(self):
        v = self.__get('select buffers_backend_fsync from pg_stat_bgwriter;')[0]
        return self.newFalconData(key='bgwriter_buffersbackendfsync', val=v)

    def get_bgwriter_write_time(self):
        v = self.__get('select checkpoint_write_time from pg_stat_bgwriter;')[0]
        return self.newFalconData(key='bgwriter_write_time', val=v)

    def get_bgwriter_sync_time(self):
        v = self.__get('select checkpoint_sync_time from pg_stat_bgwriter;')[0]
        return self.newFalconData(key='bgwriter_sync_time', val=v)

    def get_locks(self):
        v = self.__get('select count(*) from pg_locks;')[0]
        return self.newFalconData(key='locks', val=v)


    def get_sql_by_exec_time(self):
        sql = 'SELECT calls, total_time, rows, 100.0 * shared_blks_hit /nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent,substr(query,1,25)'
        sql += 'FROM pg_stat_statements ORDER BY total_time DESC LIMIT 5;'
        v = self.__get_many(sql)
        return self.newFalconData(key='sql_by_exec_time', val=v)

    # def get_table_in_buffers(self):
    #     sql = 'SELECT c.relname, count(*) AS buffers'
    #     sql += 'FROM pg_buffercache b INNER JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)'
    #     sql += ' AND b.reldatabase IN (0, (SELECT oid FROM pg_database WHERE datname = current_database())) GROUP BY c.relname ORDER BY 2 DESC LIMIT 5;'
    #     v = self.__get(sql)[0]
    #     return self.newFalconData(key='table_in_buffers', val=v)

    def get_repl_state(self):
        v = self.__get('select state from pg_stat_replication;')
        return self.newFalconData(key='repl_state', val=v)

    def get_sync_state(self):
        v = self.__get('select sync_state from pg_stat_replication;')
        return self.newFalconData(key='sync_state', val=v)


def checkNotNull(v,msg):
    if(v in ('',None)):
        print '%s is empty!' % (msg)
        sys.exit(2)

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

    global debug,timestamp,falconAgentUrl,step,metric,alwaysSend,defaultDataWhenFailed,host,port,user,pswd,db,endPoint
    for opt, arg in opts:
        if opt in ('-H','--help'):
            usage()
        if opt == '-t':
            try:
                step = int(arg)
            except Exception:
                print e
                sys.exit(2)
        elif opt == '-f':
            falconAgentUrl = arg
        elif opt == '-m':
            metric = 'postgresql'
        elif opt == '-a':
            alwaysSend = arg.lower() == 'true' and True or False
        elif opt == '-e':
            endPoint = arg
        elif opt == '-v':
            try:
                defaultDataWhenFailed = int(arg)
            except Exception:
                print e
                sys.exit(2)       
        elif opt == '-h':
            if(arg.find(":") == -1):
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
    print '[postgralcon]%s@%s:%s/%s %s %s' %(user,host,port,db,falconAgentUrl,metric)
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
                if(debug):
                    print '%s %s' % (key,d['Value'])
                data.append(d)
            else:
                if(debug):
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