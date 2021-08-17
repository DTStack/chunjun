CREATE TABLE source
(
    t_tinyint        smallint,
    t_smallint      smallint,
    t_integer     integer,
    t_bigint   BIGINT,
    t_decimal       decimal(38,18),
    t_smalldecimal decimal(38,18),
    t_real   float ,
    t_double double
) WITH (
      'connector' = 'saphana-x',
      'url' = 'jdbc:sap://localhost:39015',
      'table-name' = 'T_NUMERIC_SOURCE',
      'username' = 'SYSTEM',
      'password' = 'Abc!@#579'
      ,'scan.parallelism' = '1' -- 并行度大于1时，必须指定scan.partition.column。默认：1
      ,'scan.fetch-size' = '2' -- 每次从数据库中fetch大小。默认：1024条
      ,'scan.query-timeout' = '10' -- 数据库连接超时时间。默认：1秒
      ,'scan.partition.column' = 'id' -- 多并行度读取的切分字段，多并行度下必需要设置。无默认
      ,'scan.partition.strategy' = 'range' -- 数据分片策略。默认：range
      -- ,'scan.increment.column' = 'id' -- 增量字段名称，如果配置了该字段，目前并行度只能为1。非必填，无默认
      -- ,'scan.increment.column-type' = 'int' -- 增量字段类型。非必填，无默认
      ,'scan.start-location' = '109' -- 增量字段开始位置。非必填，无默认
      );

CREATE TABLE sink
(
    t_tinyint        smallint,
    t_smallint      smallint,
    t_integer     integer,
    t_bigint   BIGINT,
    t_decimal       decimal(38,18),
    t_smalldecimal decimal(38,18),
    t_real   float,
    t_double double
) WITH (
      'connector' = 'saphana-x',
      'url' = 'jdbc:sap://localhost:39015',
      'table-name' = 'T_NUMERIC_SINK',
      'username' = 'SYSTEM',
      'password' = 'Abc!@#579',
      'sink.buffer-flush.max-rows' = '1024', -- 批量写数据条数，默认：1024
      'sink.buffer-flush.interval' = '10000', -- 批量写时间间隔，默认：10000毫秒
      'sink.all-replace' = 'true', -- 解释如下(其他rdb数据库类似)：默认：false
      -- sink.all-replace = 'true' 生成如：REPLACE INTO `result3`(`mid`, `mbb`, `sid`, `sbb`) VALUES (?, ?, ?, ?) 。会将所有的数据都替换。
      -- sink.all-replace = 'false' 生成如：INSERT INTO `result3`(`mid`, `mbb`, `sid`, `sbb`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `mid`=IFNULL(VALUES(`mid`),`mid`), `mbb`=IFNULL(VALUES(`mbb`),`mbb`), `sid`=IFNULL(VALUES(`sid`),`sid`), `sbb`=IFNULL(VALUES(`sbb`),`sbb`) 。如果新值为null，数据库中的旧值不为null，则不会覆盖。
      'sink.parallelism' = '1'    -- 写入结果的并行度，默认：null
      );

insert into sink
select *
from source;
