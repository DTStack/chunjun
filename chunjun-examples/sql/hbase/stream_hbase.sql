CREATE TABLE source
(
    rowkey VARCHAR,
    item_id VARCHAR,
    category_id VARCHAR,
    behavior VARCHAR,
    ts TIMESTAMP(3)
) WITH (
    'connector' = 'stream-x'
);

CREATE TABLE sink
(
    rowkey VARCHAR,
    cf ROW(item_id VARCHAR, category_id VARCHAR, behavior VARCHAR, ts TIMESTAMP(3)),
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

insert into sink
SELECT rowkey, ROW(item_id, category_id, behavior, ts ) as cf
from source u;
