# StarRocks Lookup

## 一、介绍

StarRocks维表，支持全量和异步方式。<br />
全量缓存:将维表数据全部加载到内存中，建议数据量不大使用。<br />
异步缓存:使用异步方式查询数据，并将查询到的数据使用lru缓存到内存中，建议数据量大使用。

## 二、支持版本

StarRocks 2.x

## 三、插件名称

| SQL | starrocks-x |
| --- |-------------|

## 四、参数说明

- **url**
    - 描述：StarRocks JdbcUrl
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br />

- **table-name**
    - 描述：表名
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br />

- **schema-name**
    - 描述：schema
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br/>

- **username**
    - 描述：用户名
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br />

- **password**
    - 描述：密码
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br/>

- **max-retries**
    - 描述：be获取连接、读取数据的重试次数
    - 必选：否
    - 字段类型：int
    - 默认值：3

<br/>

- **scan.be.client.timeout**
    - 描述：与be通信允许的超时毫秒值
    - 必选：否
    - 参数类型：int
    - 默认值：3000

<br />

- **scan.be.client.keep-live-min**
    - 描述：be连接存活时间
    - 必选：否
    - 参数类型：int
    - 默
<br />

- **scan.be.query.timeout-s**
    - 描述：be查询超时时长
    - 必选：否
    - 参数类型：int
    - 默认值：600

<br />

- **scan.be.fetch-rows**
    - 描述：be批量读取的数据条数
    - 必选：否
    - 参数类型：int
    - 默认值：1024

<br />

- **scan.be.fetch-bytes-limit**
    - 描述：be批量读取的数据最大byte值
    - 必选：否
    - 参数类型：long
    - 默认值：1073741824 (1G)
      <br />

- **scan.be.param.properties**
    - 描述：连接be的其他可配置参数
    - 必选：否
    - 参数类型：map
    - 默认值：无
      <br />

- **scan.be.host-mapping**
    - 描述：be节点host映射
    - 必选：否
    - 参数类型：list
    - 默认值：无
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

- **lookup.max-retries**
    - 描述：LRU维表查找数据库失败时的最大重试次数
    - 必选：否
    - 参数类型：int
    - 默认值：3
      <br />

- **lookup.error-limit**
    - 描述：LRU维表发生超时、ALL Cache维表发送数据失败次数的容忍值
    - 必选：否
    - 参数类型：int
    - 默认值：Long.MAX_VALUE
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

## 五、数据类型
| **Flink type**                    | **StarRocks type** | **ChunJun Column** |
| --------------------------------- | ------------------ |--------------------|
| BOOLEAN                           | BOOLEAN            | BooleanColumn      |
| TINYINT                           | TINYINT            | ByteColumn         |
| SMALLINT                          | SMALLINT           | BigDecimalColumn   |
| INTEGER                           | INTEGER            | BigDecimalColumn   |
| BIGINT                            | BIGINT             | BigDecimalColumn   |
| FLOAT                             | FLOAT              | BigDecimalColumn   |
| DOUBLE                            | DOUBLE             | BigDecimalColumn   |
| DECIMAL                           | DECIMAL            | BigDecimalColumn   |
| BINARY                            | INTEGER            | BigDecimalColumn   |
| CHAR                              | STRING             | StringColumn       |
| VARCHAR                           | STRING             | StringColumn       |
| STRING                            | STRING             | StringColumn       |
| STRING                            | LARGEINT           | StringColumn       |
| DATE                              | DATE               | SqlDateColumn      |
| TIMESTAMP_WITHOUT_TIME_ZONE(N)    | DATETIME           | TimestampColumn    |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE(N) | DATETIME           | TimestampColumn    |
| ARRAY<T>                          | ARRAY<T>           | 暂不支持               |
| MAP<KT,VT>                        | JSON STRING        | 暂不支持               |
| ROW<arg T...>                     | JSON STRING        | 暂不支持               |

## 六、脚本示例

见项目内`chunjun-examples`文件夹。
