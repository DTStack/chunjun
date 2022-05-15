CREATE TABLE hbase_source(
     family1 ROW<col1 INT>,
     family2 ROW<col1 STRING, col2 BIGINT>,
     family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>,
     rowkey INT,
    PRIMARY KEY (rowkey) NOT ENFORCED
)
WITH(
    'connector' = 'hbase14-x'
    ,'properties.hbase.client.zookeeper.quorum' = 'kudu1:2181,kudu2:2181,kudu3:2181'
    ,'properties.zookeeper.quorum' = 'kudu1:2181,kudu2:2181,kudu3:2181'
    ,'properties.zookeeper.znode.parent' = '/hbase_2.x'
    ,'null-string-literal' = 'null'
    ,'table-name' = 'testsource'
    );


CREATE TABLE hbase_sink(
       family1 ROW<col1 INT>,
       family2 ROW<col1 STRING, col2 BIGINT>,
       family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>,
       rowkey INT,
        PRIMARY KEY (rowkey) NOT ENFORCED)
WITH(
    'connector' = 'hbase14-x'
    ,'properties.zookeeper.quorum' = 'kudu1:2181,kudu2:2181,kudu3:2181'
    ,'properties.hbase.client.zookeeper.quorum' = 'kudu1:2181,kudu2:2181,kudu3:2181'
    ,'properties.zookeeper.znode.parent' = '/hbase_2.x'
    ,'null-string-literal' = 'null'
    ,'sink.buffer-flush.max-size' = '1000'
    ,'sink.buffer-flush.max-rows' = '1000'
    ,'sink.buffer-flush.interval' = '60'
    ,'table-name' = 'testsink'
    );


INSERT INTO hbase_sink
SELECT *
FROM hbase_source;
