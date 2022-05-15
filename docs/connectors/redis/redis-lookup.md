# Redis Lookup

## 一、介绍
redis维表，支持全量和异步方式<br />
全量缓存:将维表数据全部加载到内存中，建议数据量不大使用。<br />
异步缓存:使用异步方式查询数据，并将查询到的数据使用lru缓存到内存中，建议数据量大使用。

## 二、支持版本
主流版本


## 三、插件名称
| SQL | redis-x |
| --- | --- |

## 四、参数说明
- **connector**
  - 描述：redis-x
  - 必选：是
  - 参数类型：string
  - 默认值：无
<br />

- **url**
  - 描述：localhost:6379
  - 必选：是
  - 参数类型：string
  - 默认值：无
<br />

- **table-name**
  - 描述：tableName
  - 必选：是
  - 参数类型：string
  - 默认值：无
<br />

- **password**
  - 描述：password
  - 必选：否
  - 参数类型：string
  - 默认值：无
<br />

- **redis-type**
  - 描述： redis模式（1 单机，2 哨兵， 3 集群），默认：1
  - 必选：否
  - 参数类型：string
  - 默认值：1
<br />

- **master-name**
  - 描述： 主节点名称（哨兵模式下为必填项）
  - 必选：否
  - 参数类型：string
  - 默认值：无
<br />

- **database**
  - 描述： redis 的数据库地址，默认：0
  - 必选：否
  - 参数类型：string
  - 默认值：0
<br />

- **timeout**
  - 描述：连接超时时间，默认：10000毫秒
  - 必选：否
  - 参数类型：string
  - 默认值：10000
<br />

- **max.total**
  - 描述：最大连接数 ，默认：8
  - 必选：否
  - 参数类型：string
  - 默认值：8
<br />

- **max.idle**
  - 描述：最大空闲连接数，默认：8
  - 必选：否
  - 参数类型：string
  - 默认值：8
<br />

- **min.idle**
  - 描述： 最小空闲连接数 ，默认：0
  - 必选：否
  - 参数类型：string
  - 默认值：0
<br />

- **lookup.cache-type**
  - 描述：维表缓存类型(NONE、LRU、ALL)，默认LRU
  - 必选：否
  - 参数类型：string
  - 默认值：LRU
<br />
    
- **lookup.cache-period**
  - 描述：ALL维表每隔多久加载一次数据，默认3600000毫秒(一个小时)
  - 必选：否
  - 参数类型：string
  - 默认值：3600000
<br />

- **lookup.cache.max-rows**
  - 描述：lru维表缓存数据的条数，默认10000条
  - 必选：否
  - 参数类型：string
  - 默认值：10000
<br />

- **lookup.cache.ttl**
  - 描述：lru维表缓存数据的时间，默认60000毫秒(一分钟)
  - 必选：否
  - 参数类型：string
  - 默认值：60000
<br />

- **lookup.parallelism**
  - 描述：维表并行度
  - 必选：否
  - 参数类型：string
  - 默认值：无
<br />
    
## 五、数据类型
| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |


## 六、脚本示例
见项目内`chunjun-examples`文件夹。
