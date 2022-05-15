CREATE TABLE source
(
    id          INT
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
) WITH (
      'connector' = 'stream-x'
      ,'number-of-rows' = '10'
      );

CREATE TABLE sink
(
    id          INT
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
    , primary key (id) NOT ENFORCED -- redis必须要填写，存入redis的结构是hash结构，key=tableName_primaryKey1_primaryKey2
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
      -- ,'keyExpiredTime' = '1000' -- redis sink的key的过期时间。默认是0（永不过期），单位是s。默认：0
      ,'sink.parallelism' = '3' -- sink并行度
      ,'type' = 'hash'
      ,'mode' = 'hset'
      );


INSERT INTO sink
SELECT id
     , max(name)      as name
     , max(money)     as money
     , max(dateone)   as dateone
     , max(age)       as age
     , max(datethree) as datethree
     , max(datesix)   as datesix
     , max(datenigth) as datenigth
     , max(dtdate)    as dtdate
     , max(dttime)    as dttime

     , max(afloat)    as afloat
     , max(adouble)   as adouble
     , max(aboolean)  as aboolean
     , max(abigint)   as abigint
     , max(atinyint)  as atinyint
     , max(avarchar)  as avarchar
     , max(asmallint) as asmallint
from source
group by id;
