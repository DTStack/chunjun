-- 请求一次 解析一次请求的多条数据
CREATE TABLE source
(
    axis             varchar,
    `value`               integer,
    createTime                varchar
) WITH (
      'connector' = 'http-x'
      ,'url' = 'http://localhost:8088/api/array'
      ,'intervalTime'= '3000'
      ,'method'='get'                              --请求方式：get 、post
      ,'decode'='offline-json'                             -- 数据格式：只支持json模式
      ,'header'='[ {"key":"headerName1","value":"headerValue1"},{"key":"headerName2","value":"headerValue2"}]'      -- 请求header
      ,'body'='[ {"key":"bodyName1","value":"bodyValue1"} ]'        -- 请求体
      ,'params'='[ {"key":"paramsKey1","value":"paramsValue1"} ]'      -- 请求参数：用于拼接url
      ,'returnedDataType'='array'                 -- 返回数据类型：single：单条数据；array：数组数据
      ,'jsonPath'='$'                        -- json路径：jsonpath用于解析指定层的json数据
      );

CREATE TABLE sink
(
    axis             varchar,
    `value`            integer,
    createTime       varchar
) WITH (
      'connector' = 'print'
      );


insert into sink
select *
from source ;

--  测试用例说明
--  $ 选择的是book数组下的多条数据
--
-- +I[series1, 9191352, 2023-01-04 00:07:20]
-- +I[series1, 6645322, 2023-01-04 00:14:47]
-- +I[series1, 2078369, 2023-01-04 00:22:13]
-- +I[series1, 7325410, 2023-01-04 00:29:30]
-- +I[series1, 7448456, 2023-01-04 00:37:04]
-- +I[series1, 5808077, 2023-01-04 00:44:30]
-- +I[series1, 5625821, 2023-01-04 00:52:06]
--   {
--     "axis": "series1",
--     "value": 9191352,
--     "createTime": "2023-01-04 00:07:20"
--   },
--   {
--     "axis": "series1",
--     "value": 6645322,
--     "createTime": "2023-01-04 00:14:47"
--   },
--   {
--     "axis": "series1",
--     "value": 2078369,
--     "createTime": "2023-01-04 00:22:13"
--   },
--   {
--     "axis": "series1",
--     "value": 7325410,
--     "createTime": "2023-01-04 00:29:30"
--   },
--   {
--     "axis": "series1",
--     "value": 7448456,
--     "createTime": "2023-01-04 00:37:04"
--   },
--   {
--     "axis": "series1",
--     "value": 5808077,
--     "createTime": "2023-01-04 00:44:30"
--   },
--   {
--     "axis": "series1",
--     "value": 5625821,
--     "createTime": "2023-01-04 00:52:06"
--   }
-- ]

