CREATE CATALOG hive_tudou WITH (
    'type' = 'hive',
    'default-database' = 'tudou',
    'hive-conf-dir' = '/opt/dtstack/hive/conf'
    ,'hadoop-conf-dir' = '/opt/dtstack/hadoop-2.7.6/etc/hadoop'
);

CREATE TABLE source
(
    id        INT,
    PROCTIME AS PROCTIME()
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'tudou'
      ,'properties.bootstrap.servers' = 'ip:9092'
      ,'scan.startup.mode' = 'latest-offset'
      ,'value.format' = 'json'
      );

CREATE TABLE sink
(
    id          int,
    user_id     int,
    name        varchar
    , PRIMARY KEY (id) NOT ENFORCED  -- 如果定义了，则根据该字段更新。否则追加
) WITH (
      'connector' = 'stream-x',
      'print' = 'true'
      );

create
TEMPORARY view view_out
  as
select u.id
     , s.user_id
     , s.name
from source u
         left join hive_tudou.tudou.kudu_parquet
    /*+ OPTIONS('streaming-source.enable'='true',               --SQL Hints方式，需要在confProp中配置table.dynamic-table-options.enabled=true
        'streaming-source.partition.include' = 'latest',
        'streaming-source.monitor-interval' = '1 h',
        'streaming-source.partition-order' = 'partition-name') */
    FOR SYSTEM_TIME AS OF u.PROCTIME AS s
                   on u.id = s.id;

insert into sink
select *
from view_out;
