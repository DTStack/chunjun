CREATE TABLE source
(
    id                                  decimal(38,0) ,
    t_binary_double                     double ,
    t_binary_float                      float ,
    t_char                              string ,
    t_char_varying                      string ,
    t_character                         string ,
    t_character_varying                 string ,
    t_date                              date ,
    t_decimal                           decimal(38,0),
    t_double_precision                  decimal(38,0)  ,
    t_float                             decimal(38,0) ,
    t_int                               decimal(38,0) ,
    t_integer                           decimal(38,0) ,
    t_long                              string ,
    t_national_char                     string ,
    t_national_char_varying             string ,
    t_national_character                string ,
    t_national_character_varying        string ,
    t_nchar                             string ,
    t_nchar_varying                     string ,
    t_number_1                          decimal(38,0) ,
    t_number_2                          decimal(38,0) ,
    t_number_3                          decimal(38,0) ,
    t_numeric                           decimal(38,0) ,
    t_nvarchar2                         string,
    t_raw                               bytes ,
    t_real                              decimal(38,0) ,
    t_timestamp                         timestamp ,
    t_varchar                           string ,
    t_varchar2                          string,
    PROCTIME AS PROCTIME()
) WITH (
      'connector' = 'oracle-x',
      'url' = 'jdbc:oracle:thin:@localhost:1521:orcl',
      'table-name' = 'oracle_all_type_lookup',
      'username' = 'oracle',
      'password' = 'oracle',
      'scan.fetch-size' = '2',
      'scan.query-timeout' = '10',
      'scan.start-location' = '1000',
      'scan.increment.column' = 'id',
      'scan.increment.column-type' = 'decimal'
      );

CREATE TABLE side
(
    id                                  decimal(38,0) ,
    t_binary_double                     double ,
    t_binary_float                      float ,
    t_char                              string ,
    t_char_varying                      string ,
    t_character                         string ,
    t_character_varying                 string ,
    t_date                              date ,
    t_decimal                           decimal(38,0),
    t_double_precision                  decimal(38,0)  ,
    t_float                             decimal(38,0) ,
    t_int                               decimal(38,0) ,
    t_integer                           decimal(38,0) ,
    t_long                              string ,
    t_national_char                     string ,
    t_national_char_varying             string ,
    t_national_character                string ,
    t_national_character_varying        string ,
    t_nchar                             string ,
    t_nchar_varying                     string ,
    t_number_1                          decimal(38,0) ,
    t_number_2                          decimal(38,0) ,
    t_number_3                          decimal(38,0) ,
    t_numeric                           decimal(38,0) ,
    t_nvarchar2                         string,
    t_raw                               bytes ,
    t_real                              decimal(38,0) ,
    t_timestamp                         timestamp ,
    t_varchar                           string ,
    t_varchar2                          string,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'oracle-x',
      'url' = 'jdbc:oracle:thin:@localhost:1521:orcl',
      'table-name' = 'oracle_all_type_lookup',
      'username' = 'oracle',
      'password' = 'oracle',
      'lookup.cache-type' = 'lru'
--       'lookup.cache-type' = 'all'
      );

CREATE TABLE sink
(
    id                                  decimal(38,0) ,
    t_binary_double                     double ,
    t_binary_float                      float ,
    t_char                              string ,
    t_char_varying                      string ,
    t_character                         string ,
    t_character_varying                 string ,
    t_date                              date ,
    t_decimal                           decimal(38,0),
    t_double_precision                  decimal(38,0)  ,
    t_float                             decimal(38,0) ,
    t_int                               decimal(38,0) ,
    t_integer                           decimal(38,0) ,
    t_long                              string ,
    t_national_char                     string ,
    t_national_char_varying             string ,
    t_national_character                string ,
    t_national_character_varying        string ,
    t_nchar                             string ,
    t_nchar_varying                     string ,
    t_number_1                          decimal(38,0) ,
    t_number_2                          decimal(38,0) ,
    t_number_3                          decimal(38,0) ,
    t_numeric                           decimal(38,0) ,
    t_nvarchar2                         string,
    t_raw                               bytes ,
    t_real                              decimal(38,0) ,
    t_timestamp                         timestamp ,
    t_varchar                           string ,
    t_varchar2                          string
) WITH (
      'connector' = 'oracle-x',
      'url' = 'jdbc:oracle:thin:@localhost:1521:orcl',
      'table-name' = 'oracle_all_type_sink',
      'username' = 'oracle',
      'password' = 'oracle',
      'sink.buffer-flush.max-rows' = '2000',
      'sink.all-replace' = 'true',
      'sink.buffer-flush.interval' = '0'
      );


create
TEMPORARY view view_out
  as
select s.id             AS id,
       u.t_binary_double as t_binary_double,
       u.t_binary_float as t_binary_float,
       u.t_char as t_char,
       u.t_char_varying as t_char_varying,
       u.t_character as t_character,
       u.t_character_varying as t_character_varying,
       u.t_date as t_date,
       u.t_decimal as t_decimal,
       u.t_double_precision as t_double_precision,
       u.t_float as t_float,
       u.t_int as t_int,
       u.t_integer as t_integer,
       u.t_long as t_long,
       u.t_national_char as t_national_char,
       u.t_national_char_varying as t_national_char_varying,
       u.t_national_character as t_national_character,
       u.t_national_character_varying as t_national_character_varying,
       u.t_nchar as t_nchar,
       u.t_nchar_varying as t_nchar_varying,
       u.t_number_1 as t_number_1,
       u.t_number_2 as t_number_2,
       u.t_number_3 as t_number_3,
       u.t_numeric as t_numeric,
       u.t_nvarchar2 as t_nvarchar2,
       u.t_raw as t_raw,
       u.t_real as t_real,
       u.t_timestamp as t_timestamp,
       u.t_varchar as t_varchar,
       u.t_varchar2 as t_varchar2
from source u
         inner join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s
                   on u.id = s.id;

insert into sink
select *
from view_out;
