CREATE TABLE source  (
  `col_bit` BOOLEAN,
  `col_id`  int,
  `col_bool` BOOLEAN,
  `col_boolean` BOOLEAN,
  `col_smallint` smallint,
  `col_mediumint` INTEGER,
  `col_int` int,
  `col_bigint` bigint,
  `col_integer` integer,
  `col_float`   float,
  `col_double`   double,
  `col_decimal` decimal,
  `col_date` date,
  `col_datetime` timestamp,
  `col_time` time,
  `col_timestamp` timestamp,
--  `col_year` date,
  `col_char` char,
  `col_varchar` varchar,
  `col_text` string,
  `col_tinytext` string,
  `col_mediumtext` string,
  `col_longtext` string,
  `col_binary`  BYTES,
  `col_varbinary` BYTES,
  `col_tinyblob` BYTES,
  `col_blob` BYTES,
  `col_mediumblob` BYTES,
  `col_longblob` BYTES,
  `col_enum` string,
  `col_set`  string,
  `col_json` string,
   PROCTIME AS PROCTIME()
)with(
	  'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://127.0.0.1:3306/dev',
      'table-name' = 'test',
      'username' = 'username',
      'password' = 'password'
);

CREATE TABLE side  (
  `col_int` int,
  `col_year` string,
  PRIMARY KEY (col_int) NOT ENFORCED
)with(
	  'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://127.0.0.1:3306/dev',
      'table-name' = 'year',
      'username' = 'username',
      'password' = 'password'
);

CREATE TABLE sink  (
  `col_id`  int,
  `col_bit` BOOLEAN,
  `col_bool` BOOLEAN,
  `col_boolean` BOOLEAN,
  `col_smallint` smallint,
  `col_mediumint` INTEGER,
  `col_int` int,
  `col_bigint` bigint,
  `col_integer` integer,
  `col_float`   float,
  `col_double`   double,
  `col_decimal` decimal,
  `col_date` date,
  `col_datetime` timestamp,
  `col_time` time,
  `col_timestamp` timestamp,
  `col_year` string,
  `col_char` char,
  `col_varchar` varchar,
  `col_text` string,
  `col_tinytext` string,
  `col_mediumtext` string,
  `col_longtext` string,
  `col_binary`  BYTES,
  `col_varbinary` BYTES,
  `col_tinyblob` BYTES,
  `col_blob` BYTES,
  `col_mediumblob` BYTES,
  `col_longblob` BYTES,
  `col_enum` string,
  `col_set`  string,
  `col_json` string
)with(
      'connector' = 'tidb-x',
      'url' = 'jdbc:mysql://127.0.0.1:4000/db_dev',
      'table-name' = 'sink',
      'username' = 'username',
      'password' = 'password'
);


create
TEMPORARY view view_out
  as
select u.col_id
     , u.col_bit
     , u.col_bool
     , u.col_boolean
     , u.col_smallint
     , u.col_mediumint
     , u.col_int
     , u.col_bigint
     , u.col_integer
     , u.col_float
     , u.col_double
     , u.col_decimal
     , u.col_date
     , u.col_datetime
     , u.col_time
     , u.col_timestamp
	 , s.col_year
     , u.col_char
     , u.col_varchar
     , u.col_text
     , u.col_tinytext
     , u.col_mediumtext
	 , u.col_longtext
	 , u.col_binary
	 , u.col_varbinary
	 , u.col_tinyblob
	 , u.col_blob
	 , u.col_mediumblob
	 , u.col_longblob
	 , u.col_enum
	 , u.col_set
	 , u.col_json
from source u
         left join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s
                   on u.col_id = s.col_int;

insert into sink
select *
from view_out;
