# Kudu Source

## 一、介绍

读取kudu数据

## 二、支持版本

Kudu 1.14.0

## 三、插件名称

| Sync | kudusource、kudureader |
| --- | --- |
| SQL | kudu-x |

## 四、参数说明

### 1、数据同步

- **masters**
    - 描述：kudu的IP地址和端口
    - 必选：是
    - 参数类型：string
    - 默认值：无
      

- **table**
    - 描述：要写入kudu表名
    - 必选：是
    - 参数类型：string
    - 默认值：无
      

- **workerCount**
    - 描述：kudu worker的数量
    - 必选：否
    - 参数类型：int
    - 默认值：2
      

- **operationTimeout**
    - 描述：kudu operation超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      

- **adminOperationTimeout**
    - 描述：admin operation 的超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      

- **queryTimeout**
    - 描述：query operation 的超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      

- **column**
    - 描述：需要读取的字段。
    - 属性说明:
        - name：字段名称
        - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
    - 必选：是
    - 字段类型：List
    - 默认值：


- **readMode**
    - 描述：kudu 读取模式
    - 属性说明： kudu读取模式：
        - READ_LATEST 默认的读取模式 该模式下，服务器将始终在收到请求时返回已提交的写操作。这种类型的读取不会返回快照时间戳，并且不可重复。 用ACID术语表示，它对应于隔离模式：“读已提交”

        - READ_AT_SNAPSHOT 该模式下，服务器将尝试在提供的时间戳上执行读取。如果未提供时间戳，则服务器将当前时间作为快照时间戳。 在这种模式下，读取是可重复的，即将来所有在相同时间戳记下的读取将产生相同的数据。
          执行此操作的代价是等待时间戳小于快照的时间戳的正在进行的正在进行的事务，因此可能会导致延迟损失。用ACID术语，这本身就相当于隔离模式“可重复读取”。
          如果对已扫描tablet的所有写入均在外部保持一致，则这对应于隔离模式“严格可序列化”。 注意：当前存在“空洞”，在罕见的边缘条件下会发生，通过这种空洞有时即使在采取措施使写入如此时，它们在外部也不一致。
          在这些情况下，隔离可能会退化为“读取已提交”模式。

        - READ_YOUR_WRITES 不支持该模式。
    - 必选：否
    - 字段类型：String
    - 默认值：READ_AT_SNAPSHOT


- **batchSizeBytes**
    - 描述：scanner在每批中返回的最大字节数。最大不能超过 1G。
    - 必选：否
    - 字段类型：int
    - 默认值：0


- **filter**
    - 描述：过滤条件，采用json格式，通过该配置型来限制返回 kudu 数据范围，语法请参考[kudu查询语法](https://docs.kudu.com/manual/crud/#read-operations)
    - 必选：否
    - 字段类型：String
    - 默认值：无

### 2、SQL

- **masters**
    - 描述：kudu的IP地址和端口, 必须不为 null。
    - 必选：是
    - 参数类型：string
    - 默认值：无
      

- **table-name**
    - 描述：要写入kudu表名
    - 必选：是
    - 参数类型：string
    - 默认值：无
      

- **client.worker-count**
    - 描述：kudu worker的数量
    - 必选：否
    - 参数类型：int
    - 默认值：2
      

- **client.default-operation-timeout-ms**
    - 描述：kudu operation超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      

- **client.default-admin-operation-timeout-ms**
    - 描述：admin operation 的超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      

- **scan-token.query-timeout**
    - 描述：query operation 的超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      

## 五、数据类型

|是否支持 | 类型名称 |
| --- | --- |
| 支持 | INT8、BYTE、INT16、SHORT、INT32、INT、INT64、LONG、FLOAT、DOUBLE、BOOLEAN、STRING、VARCHAR、DECIMAL、TIMESTAMP、DATE、TIME、BINARY |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |

## 六、脚本示例

见项目内`chunjun-examples`文件夹。
