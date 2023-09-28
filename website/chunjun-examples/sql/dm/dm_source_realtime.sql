--CREATE TABLE "Test"."TABLE_1"
--(
--"CHAR1" CHAR(10),
--"CHARACTER1" CHARACTER(10),
--"VARCHAR1" VARCHAR(50),
--"VARCHAR21" VARCHAR2(50),
--"NUMERIC1" NUMERIC(22,6),
--"DECIMAL1" DECIMAL(22,6),
--"BIT1" BIT,
--"INTEGER1" INTEGER,
--"INT1" INT,
--"BIGINT1" BIGINT,
--"TINYINT1" TINYINT,
--"BYTE1" BYTE,
--"SMALLINT1" SMALLINT,
--"DOUBLE1" DOUBLE,
--"DATE1" DATE,
--"TIME1" TIME(6),
--"TIMESTAMP1" TIMESTAMP(6),
--"DATETIME1" DATETIME(6),
--"DEC1" DEC(22,6),
--"FLOAT1" FLOAT,
--"REAL1" REAL,
--"TEXT1" TEXT) STORAGE(ON "MAIN", CLUSTERBTR) ;


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
    BIGINT1 bigint,
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
    TEXT1 VARCHAR
    ) WITH (
      'connector' = 'dm-x',
      'url' = 'jdbc:dm://127.0.0.1:5236',
      'schema' = 'Test',
      'table-name' = 'TABLE_1',
      'username' = 'SYSDBA',
      'password' = 'SYSDBA',

      'scan.increment.column' = 'INT1',
      'scan.partition.column' = 'INT1',
      'scan.polling-interval' = '3000',
      'scan.start-location' = '8',


      'scan.fetch-size' = '2',
      'scan.query-timeout' = '10'
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
    BIGINT1 bigint,
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
    TEXT1 VARCHAR
    ) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source;

