# StarRocks Sink

## 一、介绍

StarRocks Sink插件使用stream-load以json格式向数据库写入数据

## 二、支持版本

StarRocks 2.x

## 三、插件名称

| Sync | starrockswriter |
| ---- | --------------- |
| SQL  | starrocks-x     |

## 四、插件参数

### 1.Sync

- **url**
    - 描述：StarRocks JdbcUrl
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br />

- **schema**
    - 描述：数据库schema名
    - 必选：否
    - 字段类型：String
    - 默认值：无

<br />

- **table**
    - 描述：目的表的表名称
    - 必选：是
    - 字段类型：String
    - 默认值：无
      <br />

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

<br />

- **column**

    - 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]
    - 必选：是
    - 默认值：否
    - 字段类型：List
    - 默认值：无
      <br />

- **batchSize**

    - 描述：写入内部缓存的数据批大小，不代表一次写入StarRocks的数据量
    - 必选：否
    - 字段类型：int
    - 默认值：1024
      <br />

- **nameMapped**

    - 描述：配置此选项为true后，schema和table配置失效。将从上游来的数据中获取这两项值，可利用此配置实现多表写入
    - 必选：否
    - 字段类型：boolean
    - 默认值：false
      <br />

- **loadConf**

    - 描述：内部stream-load的配置

    - 必选：否

    - 字段类型：json

    - 默认值&示例

      ```
      "loadConf":{
          "httpCheckTimeoutMs":10000,//ms
          "queueOfferTimeoutMs":60000,//ms
          "queuePollTimeoutMs":60000,//ms
          "batchMaxSize":2147483648,//bytes
          "batchMaxRows":200000,
          "headProperties":{//other http head configuration
              "strict_mode":true
          }
      }
      ```

      <br />

### 2.SQL

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

- **feNodes**
    - 描述：StarRocks FrontendEngine地址
    - 必选：是
    - 参数类型：String
    - 默认值：无

<br/>

- **nameMapped**
    - 描述：配置此选项为true后，schema-name和table-name配置失效。将从上游来的数据中获取这两项值，可利用此配置实现多表写入
    - 必选：否
    - 参数类型：boolean
    - 默认值：false

<br/>

- **maxRetries**
    - 描述：stream-load写数据失败次数
    - 必选：否
    - 参数类型：int
    - 默认值：3

<br/>

- **batchSize**
    - 描述：写入内部缓存的数据批大小，不代表一次写入StarRocks的数据量
    - 必选：否
    - 参数类型：
    - 默认值：1024

<br/>

- **sink.batch.max-rows**
    - 描述：以schema+table为单位的批量写入StarRocks的最大条数
    - 必选：否
    - 参数类型：long
    - 默认值：200000L

<br />

- **sink.batch.max-bytes**
    - 描述：以schema+table为单位的批量写入StarRocks的最大byte
    - 必选：否
    - 参数类型：long
    - 默认值：2147483648L

 <br />
- **http.check.timeout**
    - 描述：检查FE节点连通性时，允许的超时时长毫秒值
    - 必选：否
    - 参数类型：int
    - 默认值：10000

<br />

- **queue.offer.timeout**
    - 描述：数据写入内部缓冲队列允许的超时时长毫秒值
    - 必选：否
    - 参数类型：int
    - 默认值：60000

<br />

- **queue.poll.timeout**
    - 描述：从内部缓冲队列读取数据允许的超时时长毫秒值
    - 必选：否
    - 参数类型：int
    - 默认值：60000

<br />

- **stream-load.head.properties**
    - 描述：自选的stream-load的http请求头配置
    - 必选：否
    - 参数类型：map
    - 默认值：无

<br />

- **sink.parallelism**
    - 描述：写入结果的并行度
    - 必选：否
    - 参数类型：String
    - 默认值：无

## 五、数据类型
| **Flink type**                    | **StarRocks type** | **Flinkx Column** |
| --------------------------------- | ------------------ | ----------------- |
| BOOLEAN                           | BOOLEAN            | BooleanColumn     |
| TINYINT                           | TINYINT            | ByteColumn        |
| SMALLINT                          | SMALLINT           | BigDecimalColumn  |
| INTEGER                           | INTEGER            | BigDecimalColumn  |
| BIGINT                            | BIGINT             | BigDecimalColumn  |
| FLOAT                             | FLOAT              | BigDecimalColumn  |
| DOUBLE                            | DOUBLE             | BigDecimalColumn  |
| DECIMAL                           | DECIMAL            | BigDecimalColumn  |
| BINARY                            | INTEGER            | BigDecimalColumn  |
| CHAR                              | STRING             | StringColumn      |
| VARCHAR                           | STRING             | StringColumn      |
| STRING                            | STRING             | StringColumn      |
| STRING                            | LARGEINT           | StringColumn      |
| DATE                              | DATE               | SqlDateColumn     |
| TIMESTAMP_WITHOUT_TIME_ZONE(N)    | DATETIME           | TimestampColumn   |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE(N) | DATETIME           | TimestampColumn   |
| ARRAY<T>                          | ARRAY<T>           | 暂不支持          |
| MAP<KT,VT>                        | JSON STRING        | 暂不支持          |
| ROW<arg T...>                     | JSON STRING        | 暂不支持          |

## 六、脚本示例

见项目内`chunjun-examples`文件夹。
