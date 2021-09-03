# DB2 Lookup

## 一、介绍
db2维表，支持全量和异步方式<br />
全量缓存:将维表数据全部加载到内存中，建议数据量不大使用。<br />
异步缓存:使用异步方式查询数据，并将查询到的数据使用lru缓存到内存中，建议数据量大使用。

## 二、支持版本



## 三、插件名称
| SQL | db2-x |
| --- | --- |

## 四、参数说明
- **connector**
  - 描述：db2-x
  - 必选：是
  - 参数类型：String
  - 默认值：无
<br />

- **url**
  - 描述：jdbc:db2://localhost:50002/DT_TEST
  - 必选：是
  - 参数类型：String
  - 默认值：无
<br />

- **table-name**
  - 描述：表名
  - 必选：是
  - 参数类型：String
  - 默认值：无：
<br />

- **username**
  - 描述：username
  - 必选：是
  - 参数类型：String
  - 默认值：无
<br />

- **password**
  - 描述：password
  - 必选：是
  - 参数类型：String
  - 默认值：无
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

- **lookup.fetch-size**
  - 描述：ALL维表每次从数据库加载的条数，默认1000条
  - 必选：否
  - 参数类型：string
  - 默认值：1000
<br />

- **lookup.parallelism**
  - 描述：维表并行度
  - 必选：否
  - 参数类型：string
  - 默认值：无
<br />

## 五、数据类型
| 支持 | INT,BIGINT,SMALLINT,DOUBLE,DECFLOAT,DECIMAL,VARCHAR,CHAR,CLOB,DECIMAL,TIMESTAMP,DATETIME,DATE,TIME,BYTES|
| --- | --- |
| 暂不支持 |  |


## 六、脚本示例
见项目内`flinkx-examples`文件夹。
