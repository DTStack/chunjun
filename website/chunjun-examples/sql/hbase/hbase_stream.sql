CREATE TABLE source
(
    rowkey VARCHAR,
    info ROW<id varchar>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase14-x'
    ,'zookeeper.quorum' = 'kudu1:2181,kudu2:2181,kudu3:2181'
    ,'zookeeper.znode.parent' = '/hbase_2.x'
    ,'null-string-literal' = 'null'
    ,'sink.buffer-flush.max-size' = '1000'
    ,'sink.buffer-flush.max-rows' = '1000'
    ,'sink.buffer-flush.interval' = '60'
    ,'table-name' = 'test'
);

CREATE TABLE sink
(
    rowkey VARCHAR
) WITH (
    'connector' = 'stream-x'
);

insert into sink
SELECT rowkey
from source u;
