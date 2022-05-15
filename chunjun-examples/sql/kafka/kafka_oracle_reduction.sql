-- debezium json
-- {"before":null,"after":{"id":1,"col_bit":true,"col_tinyint":1,"col_smallint":1,"col_mediumint":1,"col_int":1,"col_integer":1,"col_bigint":1,"col_real":1.0,"col_float":1.0,"col_decimal":1,"col_numric":1,"col_double":1.0,"col_char":"a","col_varchar":"a"},"op":"c"}
-- {"before":null,"after":{"id":2,"col_bit":true,"col_tinyint":1,"col_smallint":1,"col_mediumint":1,"col_int":1,"col_integer":1,"col_bigint":1,"col_real":1.0,"col_float":1.0,"col_decimal":1,"col_numric":1,"col_double":1.0,"col_char":"a","col_varchar":"a"},"op":"c"}
-- {"before":{"id":2,"col_bit":true,"col_tinyint":1,"col_smallint":1,"col_mediumint":1,"col_int":1,"col_integer":1,"col_bigint":1,"col_real":1.0,"col_float":1.0,"col_decimal":1,"col_numric":1,"col_double":1.0,"col_char":"a","col_varchar":"a"},"after":null,"op":"d"}
-- {"before":{"id":1,"col_bit":true,"col_tinyint":1,"col_smallint":1,"col_mediumint":1,"col_int":1,"col_integer":1,"col_bigint":1,"col_real":1.0,"col_float":1.0,"col_decimal":1,"col_numric":1,"col_double":1.0,"col_char":"a","col_varchar":"a"},"after":null,"op":"d"}
-- {"before":null,"after":{"id":1,"col_bit":false,"col_tinyint":2,"col_smallint":2,"col_mediumint":2,"col_int":2,"col_integer":2,"col_bigint":2,"col_real":2.0,"col_float":2.0,"col_decimal":2,"col_numric":2,"col_double":2.0,"col_char":"b","col_varchar":"b"},"op":"c"}
-- {"before":{"id":1,"col_bit":false,"col_tinyint":2,"col_smallint":2,"col_mediumint":2,"col_int":2,"col_integer":2,"col_bigint":2,"col_real":2.0,"col_float":2.0,"col_decimal":2,"col_numric":2,"col_double":2.0,"col_char":"b","col_varchar":"b"},"after":null,"op":"d"}
-- {"before":null,"after":{"id":2,"col_bit":false,"col_tinyint":2,"col_smallint":2,"col_mediumint":2,"col_int":2,"col_integer":2,"col_bigint":2,"col_real":2.0,"col_float":2.0,"col_decimal":2,"col_numric":2,"col_double":2.0,"col_char":"b","col_varchar":"b"},"op":"c"}
CREATE TABLE sourceIn
(
    id            bigint,
    col_bit       BOOLEAN,
    col_tinyint   tinyint,
    col_smallint  int,
    col_mediumint int,
    col_int       int,
    col_integer   int,
    col_bigint    bigint,
    col_real      double,
    col_float     float,
    col_decimal   decimal(10, 0),
    col_numric    decimal(10, 0),
    col_double    double,
    col_char      char(10),
    col_varchar   varchar(10),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'topic'
      ,'properties.bootstrap.servers' = 'ip:9092'
--       ,'scan.startup.mode' = 'latest-offset'
      ,'scan.startup.mode' = 'earliest-offset'
      ,'value.format' = 'debezium-json'
      );

CREATE TABLE SinkOne
(
    id            bigint,
    col_bit       BOOLEAN,
    col_tinyint   tinyint,
    col_smallint  int,
    col_mediumint int,
    col_int       int,
    col_integer   int,
    col_bigint    bigint,
    col_real      double,
    col_float     float,
    col_decimal   decimal(10, 0),
    col_numric    decimal(10, 0),
    col_double    double,
    col_char      char(10),
    col_varchar   varchar(10),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'oracle-x',
      'url' = 'jdbc:oracle:thin:@ip:1521:orcl',
      'sink.buffer-flush.max-rows' = '1000',
      'schema' = 'ORACLE',
      'table-name' = 'sql_cdc_test',
      'username' = 'username',
      'password' = 'password'
      );

insert into SinkOne
select *
from sourceIn u;