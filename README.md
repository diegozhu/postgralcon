open-falcon postgresql  monitor script
================================

System requirement
--------------------------------
OS：Linux

Python >= 2.6

psycopg2

upload metrics:
------------------------------------
| key |  tag | type | note |chinese|
|-----|------|------|------|-------|
postgresql.connections|port|GAUGE|number of active connections to this database|活跃连接数|
postgresql.commits|port|GAUGE|number of transactions that have been committed in this database|提交的事务数|
postgresql.rollbacks|port|GAUGE|transactions that have been rolled back in this database|回滚的事务数|
postgresql.disk_read|port|GAUGE|number of disk blocks read in this database|当前数据库读取的磁盘区块数|
postgresql.buffer_hit|port|GAUGE|number of times disk blocks were found in the buffer cache, preventing the need to read from the database|数据库磁盘读取缓存命中数|
postgresql.rows_returned|port|GAUGE|number of rows returned by queries in this database|返回数据行数|
postgresql.rows_fetched|port|GAUGE|number of rows fetched by queries in this database|读取数据行数|
postgresql.rows_inserted|port|GAUGE|number of rows inserted by queries in this database|数据插入行数|
postgresql.rows_updated|port|GAUGE|rows updated by queries in this database|数据更新行数|
postgresql.rows_deleted|port|GAUGE|rows deleted by queries in this database|数据删除行数|
postgresql.database_size|port|GAUGE|disk space used by this database|磁盘使用量|
postgresql.deadlocks|port|GAUGE|number of deadlocks detected in this database|死锁数|
postgresql.temp_bytes|port|GAUGE|amount of data written to temporary files by queries in this database|写入临时文件数据数量|
postgresql.temp_files|port|GAUGE|temporary files created by queries in this database|创建的临时文件数量|
postgresql.bgwriter.checkpoints_timed|port|GAUGE|scheduled checkpoints that were performed|执行的checkpoints|
postgresql.bgwriter.checkpoints_requested|port|GAUGE|requested checkpoints that were performed||
postgresql.bgwriter.buffers_checkpoint|port|GAUGE|buffers written during checkpoints||
postgresql.bgwriter.buffers_clean|port|GAUGE|buffers written by the background writer||
postgresql.bgwriter.maxwritten_clean|port|GAUGE|times the background writer stopped a cleaning scan due to writing too many buffers||
postgresql.bgwriter.buffers_backend|port|GAUGE|buffers written directly by a backend||
postgresql.bgwriter.buffers_alloc|port|GAUGE|buffers allocated||
postgresql.bgwriter.buffersbackendfsync|port|GAUGE|The of times a backend had to execute its own fsync call instead of the background writer||
postgresql.bgwriter.write_time|port|GAUGE|total amount of checkpoint processing time spent writing files to disk||
postgresql.bgwriter.sync_time|port|GAUGE|total amount of checkpoint processing time spent synchronizing files to disk||
postgresql.locks|port|GAUGE|locks active for this database||
postgresql.seq_scans|port|GAUGE|sequential scans initiated on this table||
postgresql.seqrowsread|port|GAUGE|live rows fetched by sequential scans||
postgresql.index_scans|port|GAUGE|index scans initiated on this table||
postgresql.indexrowsfetched|port|GAUGE|live rows fetched by index scans||
postgresql.rowshotupdated|port|GAUGE|rows HOT updated, meaning no separate index update was needed||
postgresql.live_rows|port|GAUGE|estimated number of live rows||
postgresql.dead_rows|port|GAUGE|estimated number of dead rows||
postgresql.indexrowsread|port|GAUGE|index entries returned by scans on this index||
postgresql.table_size|port|GAUGE|total disk space used by the specified table. Includes TOAST, free space map, and visibility map. Excludes indexes||
postgresql.index_size|port|GAUGE|total disk space used by indexes attached to the specified table||
postgresql.total_size|port|GAUGE|total disk space used by the table, including indexes and TOAST data||
postgresql.table.count|port|GAUGE|user tables in this database||
postgresql.max_connections|port|GAUGE|maximum number of client connections allowed to this database||
postgresql.percentusageconnections|port|GAUGE|connections to this database as a fraction of the maximum number of allowed connections||
postgresql.heapblocksread|port|GAUGE|disk blocks read from this table||
postgresql.heapblockshit|port|GAUGE|buffer hits in this table||
postgresql.indexblocksread|port|GAUGE|disk blocks read from all indexes on this table||
postgresql.indexblockshit|port|GAUGE|buffer hits in all indexes on this table||
postgresql.toastblocksread|port|GAUGE|disk blocks read from this table’s TOAST table||
postgresql.toastblockshit|port|GAUGE|buffer hits in this table’s TOAST table||
postgresql.toastindexblocks_read|port|GAUGE|disk blocks read from this table’s TOAST table index||
postgresql.toastindexblocks_hit|port|GAUGE|buffer hits in this table’s TOAST table index||

if you need to add/remove , please modify monit_keys accordingly.

usage:
--------------------------------
1. modify the lines that were comemmentted out accrodingly.
2. test : python postgralcon.py
3. add this script to crontab using crontab -e


CURRENT STATUS
---------------------------
| TODO      |  Supoorted  |
|-----------|-------------|
commits|connections|
rollbacks|database_size|
disk_read||
buffer_hit||
rows_returned||
rows_fetched||
rows_inserted||
rows_updated||
rows_deleted||
deadlocks||
temp_bytes||
temp_files||
bgwriter.checkpoints_timed||
bgwriter.checkpoints_requested||
bgwriter.buffers_checkpoint||
bgwriter.buffers_clean||
bgwriter.maxwritten_clean||
bgwriter.buffers_backend||
bgwriter.buffers_alloc||
bgwriter.buffersbackendfsync||
bgwriter.write_time||
bgwriter.sync_time||
locks||
seq_scans||
seqrowsread||
index_scans||
indexrowsfetched||
rowshotupdated||
live_rows||
dead_rows||
indexrowsread||
table_size||
index_size||
total_size||
table.count||
max_connections||
percentusageconnections||
heapblocksread||
heapblockshit||
indexblocksread||
indexblockshit||
toastblocksread||
toastblockshit||
toastindexblocks_read||
toastindexblocks_hit||

Lisence : MIT
--------------------------------