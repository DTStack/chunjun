-- {"id":1238123899121,"name":"asdlkjasjkdla998y1122","date":"1990-10-14","obj":{"time1":"12:12:43","str":"sfasfafs","lg":2324342345},"arr":[{"f1":"f1str11","f2":134},{"f1":"f1str22","f2":555}],"arr2":["a","b"],"time":"12:12:43Z","timestamp":"1990-10-14T12:12:43Z","map":{"flink":123},"mapinmap":{"inner_map":{"key":234}}}
CREATE TABLE source_ods_fact_user_ippv (
    id            BIGINT
    , name          STRING
    , `date`          DATE
    , obj           ROW<time1 TIME,str STRING,lg BIGINT>
    , str as obj.str
    , arr           ARRAY<ROW<f1 STRING,f2 INT>>
    , arr2          ARRAY<STRING>
    , `time`          TIME
    , `timestamp`     TIMESTAMP(3)
    , `map`        MAP<STRING,BIGINT>
    , mapinmap      MAP<STRING,MAP<STRING,INT>>
    , proctime as PROCTIME()
 ) WITH (
    'connector' = 'kafka'
    ,'topic' = 'test_multi_line'
    ,'properties.bootstrap.servers' = '172.16.100.109:9092'
    ,'properties.group.id' = 'dt_test'
    ,'scan.startup.mode' = 'earliest-offset'
    ,'format' = 'json'
    ,'json.timestamp-format.standard' = 'SQL'
    -- ,'json.fail-on-missing-field' = 'true'
    ,'json.ignore-parse-errors' = 'true'
);


CREATE TABLE result_total_pvuv_min
(
   id BIGINT
  , name STRING
  , `date`  DATE
  , str STRING
  , f1 STRING
  , tag STRING
  , map1 BIGINT
  , map2 INT
) WITH (
      'connector' = 'stream-x'
);

INSERT INTO result_total_pvuv_min
select
  id
  , name
  , `date`
  , str as str
  , arr[1].f1 as f1
  , tag
  , `map`['flink'] as map1
  , mapinmap['inner_map']['key'] as map2
from source_ods_fact_user_ippv CROSS JOIN UNNEST(arr2) AS t (tag)
