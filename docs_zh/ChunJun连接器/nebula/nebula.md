# nebula Source

## 一、介绍
支持从nebula离线读取 ,目前nebula-connector仅支持sql,（write端支持自动创建space和tag/edge）
nebula 维表，支持全量和异步方式
全量缓存:将维表数据全部加载到内存中，建议数据量不大使用。
异步缓存:使用异步方式查询数据，并将查询到的数据使用lru缓存到内存中，建议数据量大使用。

## 二、支持版本
nebula 3.0

## 三、插件名称
| SQL | nebula-x |


## 四、参数说明
### 2、SQL
- **connector**
  - 描述：nebula-x
  - 必选：是
  - 参数类型：String 
  - 默认值：无
<br />

- **nebula.storage-addresses**
  - 描述：nebula meta server address: localhost:9559
  - 必选：是
  - 参数类型：String 
  - 默认值：无
<br />

- **nebula.graphd-addresses**
    - 描述：nebula graphd server address: localhost:9559
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **nebula.password**
  - 描述：password
  - 必选：是
  - 参数类型：String 
  - 默认值：无：
<br />

- **nebula.username**
  - 描述：username
  - 必选：是
  - 参数类型：String 
  - 默认值：无
<br />

- **nebula.schema-name**
  - 描述：tag/edge's name
  - 必选：是
  - 参数类型：String 
  - 默认值：无
<br />

- **nebula.fatch-size**
  - 描述：每次拉取的条数
  - 必选：否
  - 参数类型：String 
  - 默认值：1000
<br />

- **nebula.space**
  - 描述：图谱space
  - 必选：否
  - 参数类型：String 
  - 默认值：无
<br />

- **read.tasks**
  - 描述：读并行度。
  - 必选：否
  - 参数类型：int 
  - 默认值：1
<br />

- **nebula.schema-type**
  - 描述：读取的schema 类型：vertex/edge。
  - 特别声明：当拉取或者写入的是vertex时，字段中必须还有vid字段；当拉取或这写入的是edge时，拉取时字段中前三个字段必须包含srcId,dstId,`rank` 并且顺序必须为srcId,dstId,`rank`；当写入时，`rank`字段可有可无，顺序必须是srcId,dstId,[`rank`]
  - 必选：否
  - 参数类型：String 
  - 默认值：无
<br />

- **nebula.start-time**
  - 描述：从那开始读取数据的时间（该时间是数据写到nebula的时间）,与nebula.end-time结合使用拉取某一时间段的数据
  - 必选：否
  - 参数类型：long 
  - 默认值：0
<br />

- **nebula.end-time**
  - 描述：扫描数据结束的时间（该时间是数据写到nebula的时间）
  - 必选：否
  - 参数类型：long 
  - 默认值：Long.MAX_VALUE
<br />

- **client.connect-timeout**
  - 描述：nabula client max connect timeout. default: 30 s
  - 必选：否
  - 参数类型：long 
  - 默认值：1000 * 30
<br />

- **nebula.is-reconnect**
  - 描述：should nebula reconnect when exception throws
  - 必选：否
  - 参数类型：boolean 
  - 默认值：false
<br />
  
- **nebula.bulk-size**
  - 描述：the rows of data each write action
  - 必选：否
  - 参数类型：int 
  - 默认值：1000
<br />

- **nebula.enableSSL**
  - 描述：should enable ssl to connect nebula server
  - 必选：否
  - 参数类型：boolean 
  - 默认值：false
<br />

- **nebula.sslParamType**
  - 描述：证书类型： self/cas
  - 必选：否
  - 参数类型：String 
  - 默认值：无
<br />

- **nebula.caCrtFilePath**
    - 描述：the caCrt File path
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **nebula.crtFilePath**
    - 描述：the crt File path
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **nebula.keyFilePath**
    - 描述：the key File path
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **nebula.sslPassword**
    - 描述：the ssl password,only self type of {SSL_PARAM_TYPE} need
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **nebula.connection-retry**
    - 描述：the times of retry to connect
    - 必选：否
    - 参数类型：int
    - 默认值：0
      <br />

- **nebula.execution-retry**
    - 描述：the times of retry to execute query
    - 必选：否
    - 参数类型：int
    - 默认值：0
      <br />

- **write.tasks**
    - 描述：witer task parallelism
    - 必选：否
    - 参数类型：int
    - 默认值：1
      <br />

- **nebula.fetch-interval**
    - 描述：Pull data within a given time, and set the fetch-interval to achieve the effect of breakpoint resuming, which is equivalent to dividing the time into multiple pull-up data according to the fetch-interval. default: 30 days
    - 必选：否
    - 参数类型：long
    - 默认值：Long.MAX_VALUE
      <br />

- **nebula.default-allow-part-success**
    - 描述：if allow part success
    - 必选：否
    - 参数类型：boolean
    - 默认值：false
      <br />

- **nebula.default-allow-read-follower**
    - 描述：if allow read from follower
    - 必选：否
    - 参数类型：boolean
    - 默认值：true
      <br />
        
- **nebula.min.conns.size**
  - 描述：The min connections in nebula pool for all addresses
  - 必选：否
  - 参数类型：int
  - 默认值：0
    <br />

- **nebula.max.conns.size**
    - 描述：The max connections in nebula pool for all addresses
    - 必选：否
    - 参数类型：int
    - 默认值：10
      <br />

- **nebula.idle.time**
    - 描述：The idleTime of the connection, unit: millisecond  The connection's idle time more than idleTime, it will be delete  0 means never delete
    - 必选：否
    - 参数类型：int
    - 默认值：0
      <br />

- **nebula.interval.idle**
    - 描述：the interval time to check idle connection, unit ms, -1 means no check
    - 必选：否
    - 参数类型：int
    - 默认值：-1
      <br />

- **nebula.wait.idle.time**
    - 描述：the wait time to get idle connection, unit ms
    - 必选：否
    - 参数类型：int
    - 默认值：0
      <br />

- **write-mode**
    - 描述：write mode : insert/upsert
    - 必选：否
    - 参数类型：String
    - 默认值：insert
      <br />

- **nebula.fix-string-len**
    - 描述：nebula fix string data type length,while create space needed
    - 必选：否
    - 参数类型：int
    - 默认值：64
      <br />

- **nebula.vid-type**
    - 描述：the vid type for create space,is space exist,this prop dose not need
    - 必选：否
    - 参数类型：String
    - 默认值：FIXED_STRING(32)
      <br />

- **lookup.cache-type**
    - 描述：维表缓存类型(LRU、ALL)，默认LRU
    - 必选：否
    - 参数类型：String
    - 默认值：FIXED_STRING(32)
      <br />
      

## 五、数据类型
| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING|


## 六、脚本示例
见项目内`chunjun-examples`文件夹。
