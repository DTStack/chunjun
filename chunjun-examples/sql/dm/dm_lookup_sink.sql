CREATE TABLE source
(
    CHAR1 CHAR,
    CHARACTER1 CHARACTER,
    VARCHAR1 VARCHAR,
    VARCHAR21 VARCHAR,
    NUMERIC1 NUMERIC,
    DECIMAL1 DECIMAL,
    BIT1 BOOLEAN,
    INTEGER1 INTEGER,
    INT1 INT,
    BIGINT1 BIGINT,
    TINYINT1 TINYINT,
    BYTE1 TINYINT,
    SMALLINT1 SMALLINT,
    DOUBLE1 DOUBLE,
    DATE1 DATE,
    TIME1 TIME,
    TIMESTAMP1 TIMESTAMP,
    DATETIME1 TIMESTAMP,
    DEC1 DECIMAL,
    FLOAT1 DOUBLE,
    REAL1 FLOAT,
    TEXT1 VARCHAR,
    PROCTIME AS PROCTIME()
    ) WITH (
      'connector' = 'dm-x',
      'url' = 'jdbc:dm://127.0.0.1:5236',
      'schema' = 'Test',
      'table-name' = 'TABLE_1',
      'username' = 'SYSDBA',
      'password' = 'SYSDBA',
      'scan.fetch-size' = '2',
      'scan.query-timeout' = '10'
      );

CREATE TABLE side
(
    INT2          INT,
    PRIMARY KEY (INT2) NOT ENFORCED
) WITH (
         'connector' = 'dm-x',
         'url' = 'jdbc:dm://127.0.0.1:5236',
         'schema' = 'Test',
         'table-name' = 'TABLE_2',
         'username' = 'SYSDBA',
         'password' = 'SYSDBA',
         'scan.fetch-size' = '2',
         'scan.query-timeout' = '10',
         'lookup.cache-type' = 'lru'
         );


CREATE TABLE sink
(
    CHAR1 CHAR,
    CHARACTER1 CHARACTER,
    VARCHAR1 VARCHAR,
    VARCHAR21 VARCHAR,
    NUMERIC1 NUMERIC,
    DECIMAL1 DECIMAL,
    BIT1 BOOLEAN,
    INTEGER1 INTEGER,
    INT1 INT,
    BIGINT1 BIGINT,
    TINYINT1 TINYINT,
    BYTE1 TINYINT,
    SMALLINT1 SMALLINT,
    DOUBLE1 DOUBLE,
    DATE1 DATE,
    TIME1 TIME,
    TIMESTAMP1 TIMESTAMP,
    DATETIME1 TIMESTAMP,
    DEC1 DECIMAL,
    FLOAT1 DOUBLE,
    REAL1 FLOAT,
    TEXT1 VARCHAR,
    INT2 INT,
    PRIMARY KEY (INT2) NOT ENFORCED
) WITH (
      'connector' = 'dm-x',
      'url' = 'jdbc:dm://127.0.0.1:5236',
      'schema' = 'Test',
      'table-name' = 'TABLE_3',
      'username' = 'SYSDBA',
      'password' = 'SYSDBA',
      'sink.buffer-flush.max-rows' = '1',
      'sink.all-replace' = 'true'
      );

create
TEMPORARY view view_out
  as
select u.CHAR1
     , u.CHARACTER1
     , u.VARCHAR1
     , u.VARCHAR21
     , u.NUMERIC1
     , u.DECIMAL1
     , u.BIT1
     , u.INTEGER1
     , u.INT1
     , u.BIGINT1
     , u.TINYINT1
     , u.BYTE1
     , u.SMALLINT1
     , u.DOUBLE1
     , u.DATE1
     , u.TIME1
     , u.TIMESTAMP1
     , u.DATETIME1
     , u.DEC1
     , u.FLOAT1
     , u.REAL1
     , u.TEXT1
     , s.INT2
from source u
         left join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s
         on u.INT1 = s.INT2;

insert into sink
select *
from view_out;
