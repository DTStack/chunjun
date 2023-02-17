CREATE TABLE sourceTable
(
    id   int,
    name varchar,
    age  int,
    proc_time AS PROCTIME()
) WITH (
      'connector' = 'stream-x',
      'number-of-rows' = '10', -- 输入条数，默认无限
      'rows-per-second' = '1' -- 每秒输入条数，默认不限制
      );

CREATE TABLE mysqlResultTable
(
    id             INT,
    tinyint_data   TINYINT,
    smallint_data  SMALLINT,
    int_data       INT,
    integer_data   INT,
    bigint_data    BIGINT,
    float_data     FLOAT,
    decimal_data   DECIMAL(10, 0),
    numeric_data   DECIMAL(10, 0),
    double_data    DOUBLE,
    varchar_data   VARCHAR,
    char_data      CHAR,
    timestamp_data TIMESTAMP,
    datetime_data  STRING,
    date_data      DATE,
    time_data      TIME,
    tinytext_data  VARCHAR,
    varbinary_data VARBINARY,
    enum_data      CHAR,
    set_data       CHAR
) WITH (
--       'password' = 'DT@Stack#123',
--       'connector' = 'mysql-x',
--       'sink.buffer-flush.interval' = '1000',
--       'sink.all-replace' = 'false',
--       'sink.buffer-flush.max-rows' = '100',
--       'table-name' = 'result_112_mysql_all_type_table',
--       'sink.parallelism' = '1',
--       'url' = 'jdbc:mysql://172.16.100.186:3306/automation',
--       'username' = 'drpeco'
      'connector' = 'stream-x',
      'print' = 'true'
      );
CREATE TABLE mysqlSideTable
(
    id             INT,
    tinyint_data   TINYINT,
    smallint_data  SMALLINT,
    int_data       INT,
    integer_data   INT,
    bigint_data    BIGINT,
    float_data     FLOAT,
    decimal_data   DECIMAL(10, 0),
    numeric_data   DECIMAL(10, 0),
    double_data    DOUBLE,
    varchar_data   VARCHAR,
    char_data      CHAR,
    timestamp_data TIMESTAMP,
    datetime_data  STRING,
    date_data      DATE,
    time_data      TIME,
    tinytext_data  VARCHAR,
    varbinary_data VARBINARY,
    enum_data      CHAR,
    set_data       CHAR,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'password' = 'DT@Stack#123',
      'connector' = 'mysql-x',
      'lookup.cache-type' = 'LRU',
      'lookup.parallelism' = '1',
      'vertx.worker-pool-size' = '5',
      'lookup.cache.ttl' = '60000',
      'lookup.cache.max-rows' = '10000',
      'table-name' = 'side_112_mysql_all_type_table',
      'url' = 'jdbc:mysql://172.16.100.186:3306/automation?useSSL=false',
      'username' = 'drpeco'
      );
INSERT
INTO mysqlResultTable
select st.id,
       mst.tinyint_data,
       mst.smallint_data,
       mst.int_data,
       mst.integer_data,
       mst.bigint_data,
       mst.float_data,
       mst.decimal_data,
       mst.numeric_data,
       mst.double_data,
       mst.varchar_data,
       mst.char_data,
       mst.timestamp_data,
       mst.datetime_data,
       mst.date_data,
       mst.time_data,
       mst.tinytext_data,
       mst.varbinary_data,
       mst.enum_data,
       mst.set_data
from sourceTable st
         join
     mysqlSideTable FOR SYSTEM_TIME as of st.proc_time as mst
     on st.id = mst.id;
