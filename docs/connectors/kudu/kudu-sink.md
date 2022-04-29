# Kudu Sink

## 一、介绍

kudu sink

## 二、支持版本

主流版本

## 三、插件名称

| Sync | kudusink、kuduwriter |
| --- | --- |
| SQL | kudu-x |

## 四、参数说明

### 1、Sync

- **masters**
    - 描述：kudu的IP地址和端口
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **table**
    - 描述：要写入kudu表名
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **column**
    - 描述：需要读取的字段。
    - 属性说明:
        - name：字段名称
        - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
    - 必选：是
    - 字段类型：List
    - 默认值：无

- **flushMode**
    - 描述：写入 kudu 时，Kudu session的 flushMode
    - 必选：否
    - 默认值：AUTO_FLUSH_SYNC
      <br />

- **maxBufferSize**
    - 描述：kudu client 中缓存数据的最大条数。
    - 注意：当kudu session 中buffer里缓存的数据条数大于maxBufferSize，kudu session 会抛出"Buffer too big " 的异常，此异常并不会影响buffer中数据的实际写入，
    - 必选：否
    - 默认值：1024
      <br />

- **flushInterval**
    - 描述：批量写入 kudu 的刷新时间。
    - 单位：毫秒
    - 必选：否
    - 默认值：10000
      <br />

- **workerCount**
    - 描述：kudu worker的数量
    - 必选：否
    - 默认值：2
      <br />

- **operationTimeout**
    - 描述：kudu operation超时时间
    - 必选：否
    - 默认值：30 * 1000（30秒）
      <br />

- **adminOperationTimeout**
    - 描述：admin operation 的超时时间
    - 必选：否
    - 默认值：30 * 1000（30秒）
      <br />

- **queryTimeout**
    - 描述：query operation 的超时时间
    - 必选：否
    - 默认值：30 * 1000（30秒）
      <br />

### 2、SQL

- **connector**
    - 描述：kudu-x
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **masters**
    - 描述：localhost:7051
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **table-name**
    - 描述：table-name
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **session.flush-mode**
    - 描述：写入 kudu 时，Kudu session的 flushMode
    - 必选：否
    - 参数类型：string
    - 默认值：AUTO_FLUSH_SYNC
      <br />

- **session.mutation-buffer-space**
    - 描述：kudu client 中缓存数据的最大条数。
    - 注意：当kudu session 中buffer里缓存的数据条数大于maxBufferSize，kudu session 会抛出"Buffer too big " 的异常，此异常并不会影响buffer中数据的实际写入，
    - 必选：否
    - 参数类型：int
    - 默认值：1024
      <br />

- **sink.buffer-flush.interval**
    - 描述：批量写入 kudu 的刷新时间。
    - 单位：毫秒
    - 必选：否
    - 参数类型：int
    - 默认值：10000
      <br />

- **client.worker-count**
    - 描述：kudu worker的数量
    - 必选：否
    - 参数类型：int
    - 默认值：2
      <br />

- **client.default-operation-timeout-ms**
    - 描述：kudu operation超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      <br />

- **client.default-admin-operation-timeout-ms**
    - 描述：admin operation 的超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      <br />

- **scan-token.query-timeout**
    - 描述：query operation 的超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      <br />

- **sink.parallelism**
    - 描述：sink并行度
    - 必选：否
    - 参数类型：string
    - 默认值：无
      <br />

## 五、数据类型

| 支持 | INT8、BYTE、INT16、SHORT、INT32、INT、INT64、LONG、FLOAT、DOUBLE、BOOLEAN、STRING、VARCHAR、DECIMAL、TIMESTAMP、DATE、TIME、BINARY |
| --- | --- |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |

## 六、脚本示例

见项目内`flinkx-examples`文件夹。
