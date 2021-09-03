-- {"id":1651,"name":"james","money":293.899778,"dateone":"2020-07-30 10:08:22","age":"33","datethree":"2020-07-30 10:08:22.123","datesix":"2020-07-30 10:08:22.123456","datenigth":"2020-07-30 10:08:22.123456789","dtdate":"2020-07-30","dttime":"11:08:22"}
CREATE TABLE source
(
    id        INT
    , name      STRING
    , money     decimal
    , dateone   timestamp
    , age       bigint
    , datethree timestamp
    , datesix   timestamp(6)
    , datenigth timestamp(9)
    , dtdate    date
    , dttime    time
    , PROCTIME AS PROCTIME()
) WITH (
      -- 'connector' = 'stream-x'

      'connector' = 'kafka-x'
      ,'topic' = 'da'
      ,'properties.bootstrap.servers' = 'kudu1:9092'
      ,'properties.group.id' = 'luna_g'
      -- ,'scan.startup.mode' = 'earliest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      );


CREATE TABLE side
(
    id        INT
    , name      STRING
    , money     decimal
    , dateone   timestamp
    , age       bigint
    , datethree timestamp
    , datesix   timestamp(6)
    , datenigth timestamp(9)
    , dtdate    date
    , dttime    time

    , afloat    float
    , adouble   double
    , aboolean  BOOLEAN
    , abigint   BIGINT
    , atinyint  TINYINT
    , avarchar  varchar
    , asmallint SMALLINT
    -- , primary key (id,name) NOT ENFORCED -- 这里的pk，并不会作为查询redis的主键，所以作为维表可不写。是通过select中的join条件作为主键
) WITH (
      'connector' = 'redis-x' --必填
      ,'url' = 'localhost:6379' --必填，格式ip:port[,ip:port]
      ,'table-name' = 'cx' --必填
      ,'password' = '123456' -- 密码 无默认，非必填项
      ,'redis-type' = '1' -- redis模式（1 单机，2 哨兵， 3 集群），默认：1
      ,'master-name' = 'lala' -- 主节点名称（哨兵模式下为必填项）
      ,'database' = '0' -- redis 的数据库地址，默认：0

      ,'timeout' = '10000' -- 连接超时时间，默认：10000毫秒
      ,'max.total' = '5' -- 最大连接数 ，默认：8
      ,'max.idle' = '5' -- 最大空闲连接数，默认：8
      ,'min.idle' = '0' -- 最小空闲连接数 ，默认：0

      ,'lookup.cache-type' = 'all' -- 维表缓存类型(NONE、LRU、ALL)，默认LRU
      ,'lookup.cache-period' = '4600000' -- ALL维表每隔多久加载一次数据，默认3600000毫秒
      ,'lookup.cache.max-rows' = '20000' -- lru维表缓存数据的条数，默认10000条
      ,'lookup.cache.ttl' = '700000' -- lru维表缓存数据的时间，默认60000毫秒
      ,'lookup.fetch-size' = '2000' -- ALL维表每次从数据库加载的条数，默认1000条
      ,'lookup.async-timeout' = '30000' -- lru维表缓访问超时时间，默认10000毫秒，暂时没用到
      ,'lookup.parallelism' = '3' -- 维表并行度，默认null
      );

-- 维表数据
-- 127.0.0.1:6379> HGETALL cx_1651
--  1) "dttime"
--  2) "14:11:18"
--  3) "adouble"
--  4) "3807.67"
--  5) "dtdate"
--  6) "2021-06-22"
--  7) "aboolean"
--  8) "true"
--  9) "datenigth"
-- 10) "2021-06-22 06:11:18.478"
-- 11) "afloat"
-- 12) "7271.44"
-- 13) "asmallint"
-- 14) "7821"
-- 15) "avarchar"
-- 16) "h1"
-- 17) "datesix"
-- 18) "2021-06-22 06:11:18.478"
-- 19) "money"
-- 20) "3059"

CREATE TABLE sink
(
    id         INT
    , name       STRING
    , money      decimal
    , dateone    timestamp
    , age        bigint
    , datethree  timestamp
    , datesix    timestamp(6)
    , datenigth  timestamp(9)
    , dtdate     date
    , dttime     time

    , aid        INT
    , aname      STRING
    , amoney     decimal
    , adateone   timestamp
    , aage       bigint
    , adatethree timestamp
    , adatesix   timestamp(6)
    , adatenigth timestamp(9)
    , adtdate    date
    , adttime    time

    , afloat    float
    , adouble   double
    , aboolean  BOOLEAN
    , abigint   BIGINT
    , atinyint  TINYINT
    , avarchar  varchar
    , asmallint SMALLINT
    -- , primary key (id, name) NOT ENFORCED
) WITH (
      'connector' = 'stream-x'
      );


INSERT INTO sink
SELECT
    u.id
     , u.name
     , u.money
     , u.dateone
     , u.age
     , u.datethree
     , u.datesix
     , u.datenigth
     , u.dtdate
     , u.dttime

     , s.id        as aid
     , s.name      as aname
     , s.money     as amoney
     , s.dateone   as adateone
     , s.age       as aage
     , s.datethree as adatethree
     , s.datesix   as adatesix
     , s.datenigth as adatenigth
     , s.dtdate    as adtdate
     , s.dttime    as adttime

     , s.afloat
     , s.adouble
     , s.aboolean
     , s.abigint
     , s.atinyint
     , s.avarchar
     , s.asmallint
from source u
         left join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s
                   on u.id = s.id;
