-- 请求多次 每次请求都为一条数据
CREATE TABLE source
(
       color             varchar,
       price             double

) WITH (
      'connector' = 'http-x'
      ,'url' = 'http://localhost:8088/api/arraypage'
      ,'intervalTime'= '3000'
      ,'method'='get'                              --请求方式：get 、post
      ,'decode'='offline-json'                             -- 数据格式：只支持json模式
      ,'header'='[ {"key":"headerName1","value":"headerValue1"},{"key":"headerName2","value":"headerValue2"}]'      -- 请求header
      ,'body'='[ {"key":"bodyName1","value":"bodyValue1"} ]'        -- 请求体
      ,'params'='[ {"key":"paramsKey1","value":"paramsValue1"} ]'      -- 请求参数：用于拼接url
      ,'returnedDataType'='single'                 -- 返回数据类型：single：单条数据；array：数组数据
      ,'jsonPath'='$.store.bicycle'                        -- json路径：jsonpath用于解析指定层的json数据
                                                   -- 以下4个参数要同时存在：
      ,'pageParamName'='pagenum'                          -- 多次请求参数1：分页参数名：例如：pageNum
      ,'startIndex'='1'                             -- 多次请求参数2：开始的位置
      ,'endIndex'='40000'                               -- 多次请求参数3：结束的位置
      ,'step'='1'                                  -- 多次请求参数4：步长：默认值为1
      );

CREATE TABLE sink
(
      color             varchar,
       price             double
) WITH (
      'connector' = 'print'
      );


insert into sink
select *
from source ;

--  $.store.book 选择的是book数组下的多条数据
--
-- source表的字段名要对应起来：category、author、title、price
-- +I[red, 19.95]
-- +I[red, 19.95]
-- +I[red, 19.95]
-- +I[red, 19.95]
-- {
--   "store": {
--     "book": [{
--       "category": "reference",
--       "author": "Nigel Rees",
--       "title": "Sayings of the Century",
--       "price": 8.95
--     }, {
--       "category": "fiction",
--       "author": "Evelyn Waugh",
--       "title": "Sword of Honour",
--       "price": 12.99
--     }, {
--       "category": "fiction",
--       "author": "Herman Melville",
--       "title": "Moby Dick",
--       "isbn": "0-553-21311-3",
--       "price": 8.99
--     }, {
--       "category": "fiction",
--       "author": "J. R. R. Tolkien",
--       "title": "The Lord of the Rings",
--       "isbn": "0-395-19395-8",
--       "price": 22.99
--     }
--     ],
--     "bicycle": {
--       "color": "red",
--       "price": 19.95
--     }
--   }
-- }
