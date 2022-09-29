# StarRocks Source

## 一、介绍

StarRocks 支持从StarRocks读取数据

## 二、支持版本

StarRocks2.x

## 三、插件名称

| Sync | starrocksreader、starrockssource |
| ---- | -------------------------------- |
| SQL  | starrocks-x                      |

## 四、参数说明

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
    - 描述：需要读取的字段。
    - 必选：是
    - 格式：支持3中格式<br />1.读取全部字段，如果字段数量很多，可以使用下面的写法：

```bash
"column":["*"]
```

- 2.只指定字段名称：

```
"column":["id","name"]
```

- 3.指定具体信息：

```json
"column": [{
    "name": "col",
    "type": "datetime",
    "format": "yyyy-MM-dd hh:mm:ss",
    "value": "value"
}]
```

<br/>

- **maxRetries**
    - 描述：be获取连接、读取数据的重试次数
    - 必选：否
    - 字段类型：int
    - 默认值：3

<br/>

- **filterStatement**
    - 描述：简单的sql过滤语句，例如id>10
    - 必选：否
    - 参数类型：string
    - 默认值：无

<br/>

- **beClientTimeout**
    - 描述：与be通信允许的超时毫秒值
    - 必选：否
    - 参数类型：int
    - 默认值：3000

<br />

- **beClientKeepLiveMin**
    - 描述：be连接存活时间
    - 必选：否
    - 参数类型：int
    - 默
      <br />

- **beQueryTimeoutSecond**
    - 描述：be查询超时时长
    - 必选：否
    - 参数类型：int
    - 默认值：600

<br />

- **beFetchRows**
    - 描述：be批量读取的数据条数
    - 必选：否
    - 参数类型：int
    - 默认值：1024

<br />

- **beFetchMaxBytes**
    - 描述：be批量读取的数据最大byte值
    - 必选：否
    - 参数类型：long
    - 默认值：1073741824 (1G)
      <br />
- **beSocketProperties**
    - 描述：连接be的其他可配置参数
    - 必选：否
    - 参数类型：map
    - 默认值：无
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

- **fe-nodes**
    - 描述：StarRocks FrontendEngine地址
    - 必选：是
    - 参数类型：String
    - 默认值：无

<br/>

- **max-retries**
    - 描述：be获取连接、读取数据的重试次数
    - 必选：否
    - 字段类型：int
    - 默认值：3

<br/>

- **filter-statement**
    - 描述：简单的sql过滤语句，例如id>10
    - 必选：否
    - 参数类型：string
    - 默认值：无

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

# 六、脚本示例

见项目内`chunjun-examples`文件夹。
