-- 解析单条数据
CREATE TABLE source
(
        axis             varchar,
    `value`               integer,
    createTime                varchar
) WITH (
      'connector' = 'http-x'
      ,'url' = 'http://localhost:8088/api/single'
      ,'intervalTime'= '3000'
      ,'method'='get'                              --请求方式：get 、post
      ,'decode'='offline-json'                             -- 数据格式：只支持json模式
      ,'header'='[ {"key":"headerName1","value":"headerValue1"},{"key":"headerName2","value":"headerValue2"}]'      -- 请求header
      ,'body'='[ {"key":"bodyName1","value":"bodyValue1"} ]'        -- 请求体
      ,'params'='[ {"key":"paramsKey1","value":"paramsValue1"} ]'      -- 请求参数：用于拼接url
      ,'returnedDataType'='single'                 -- 返回数据类型：single：单条数据；array：数组数据
      ,'jsonPath'='$'                        -- json路径：jsonpath用于解析指定层的json数据
                                                   -- 以下4个参数要同时存在：
      ,'pageParamName'=''                          -- 多次请求参数1：分页参数名：例如：pageNum
      ,'startIndex'='1'                             -- 多次请求参数2：开始的位置
      ,'endIndex'='1'                               -- 多次请求参数3：结束的位置
      ,'step'='1'                                  -- 多次请求参数4：步长：默认值为1
      );

CREATE TABLE sink
(
       axis             varchar,
    `value`               integer,
    createTime                varchar
) WITH (
      'connector' = 'print'
      );


insert into sink
select *
from source ;

--  $.store获取的是store子层的数据，有book数组，bicycle数据。
-- 当source字段名为bicycle时选择的是 bicycle下的数据，+I({color=red, price=19.95})
-- {
--   "store": {
--     "book": [{
--     }
--     ],
--     "bicycle": {
--       "color": "red",
--       "price": 19.95
--     }
--   }
-- }
