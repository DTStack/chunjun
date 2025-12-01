CREATE TABLE source
(
    category             varchar,
    author               varchar,
    title                varchar,
    price                double
) WITH (
      'connector' = 'http-x'
      ,'url' = 'http://localhost:8088/api/arraypage'
      ,'intervalTime'= '3000'
      ,'method'='get'                              --请求方式：get 、post
      ,'decode'='offline-json'                             -- 数据格式：只支持json模式
      ,'header'='[ {"key":"headerName1","value":"headerValue1"},{"key":"headerName2","value":"headerValue2"}]'      -- 请求header
      ,'body'='[ {"key":"bodyName1","value":"bodyValue1"} ]'        -- 请求体
      ,'params'='[ {"key":"paramsKey1","value":"paramsValue1"} ]'      -- 请求参数：用于拼接url
      ,'returned-data-type'='array'                 -- 返回数据类型：single：单条数据；array：数组数据
      ,'json-path'='$.store.book'                        -- json路径：jsonpath用于解析指定层的json数据
                                                   -- 以下4个参数要同时存在：
      ,'page-param-name'='pagenum'                          -- 多次请求参数1：分页参数名：例如：pageNum
      ,'start-index'='1'                             -- 多次请求参数2：开始的位置
      ,'end-index'='10000'                               -- 多次请求参数3：结束的位置
      ,'step'='1'                                  -- 多次请求参数4：步长：默认值为1
      );

CREATE TABLE sink
(
    category             varchar,
    author               varchar,
    title                varchar,
    price                double
) WITH (
      'connector' = 'print'
      );


insert into sink
select *
from source ;

--  print说明：

-- 数据原型
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

--  $.store.book 选择的是book数组下的多条数据
--
-- source表的字段名要对应起来：category、author、title、price
-- 7> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 6> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 10> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 11> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 8> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 12> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 12> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 1> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 3> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 9> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 5> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 12> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 1> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 2> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 3> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 8> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 11> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 10> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 7> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 6> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 2> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 4> +I[reference, Nigel Rees, Sayings of the Century, 8.95]
-- 3> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 1> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 10> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 12> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
-- 2> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 4> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 5> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 3> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
-- 1> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
-- 2> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
-- 4> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 5> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 9> +I[fiction, Evelyn Waugh, Sword of Honour, 12.99]
-- 7> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 4> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
-- 10> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
-- 6> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 9> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 8> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 7> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
-- 11> +I[fiction, Herman Melville, Moby Dick, 8.99]
-- 5> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
-- 9> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
-- 8> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
-- 6> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
-- 11> +I[fiction, J. R. R. Tolkien, The Lord of the Rings, 22.99]
