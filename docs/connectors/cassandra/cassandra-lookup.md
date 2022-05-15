# Cassandra Lookup

## 一、介绍

Cassandra维表，支持全量和异步方式<br />
全量缓存:将维表数据全部加载到内存中，建议数据量不大，且数据不经常变动的场景使用。<br />
异步缓存:使用异步方式查询数据，并将查询到的数据使用lru缓存到内存中，建议数据量大使用。

## 二、支持版本

Cassandra 常用版本

## 三、插件名称

| SQL | Cassandra-x |
| --- | --- |

## 四、参数说明

- **host**
    - 描述：Cassandra的IP地址，多个地址之间用逗号隔开
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **port**
    - 描述：Cassandra连接端口
    - 必选：否
    - 参数类型：int
    - 默认值：9042
      <br />

- **table-name**
    - 描述：要读取Cassandra表名
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **keyspaces**
    - 描述：Cassandra keyspaces
    - 必选：否
    - 参数类型：string
    - 默认值：无
      <br />

- **clusterName**
    - 描述：Cassandra cluster name
    - 必选：否
    - 参数类型：string
    - 默认值：chunjun-cluster
      <br />

- **consistency**
    - 描述：Cassandra consistence（一致性）
    - 说明：一致性级别决定了副本中必须有多少节点响应协调器节点才能成功处理非轻量级事务。
    - 必选：否
    - 参数类型：string
    - 默认值：LOCAL_QUORUM
      <br />

- **coreConnectionsPerHost**
    - 描述：Cassandra 每个地址可供最多可用连接数
    - 必选：否
    - 参数类型：int
    - 默认值：8
      <br />

- **maxConnectionsPerHost**
    - 描述：Cassandra 每个地址可供最多可连接数
    - 必选：否
    - 参数类型：int
    - 默认值：32768
      <br />

- **maxRequestsPerConnection**
    - 描述：Cassandra 每个连接的最多请求数
    - 必选：否
    - 参数类型：int
    - 默认值：1
      <br />

- **maxQueueSize**
    - 描述：Cassandra 队列最大数
    - 必选：否
    - 参数类型：int
    - 默认值：10000
      <br />

- **readTimeoutMillis**
    - 描述：Cassandra read 超时时长
    - 必选：否
    - 参数类型：int
    - 默认值：60 * 1000
      <br />

- **poolTimeoutMillis**
    - 描述：Cassandra pool 超时时长
    - 必选：否
    - 参数类型：int
    - 默认值：60 * 1000
      <br />

- **connectTimeoutMillis**
    - 描述：Cassandra connect 超时时长
    - 必选：否
    - 参数类型：int
    - 默认值：60 * 1000
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

| 支持 | BYTE、INT、FLOAT、DOUBLE、BOOLEAN、TEXT、VARCHAR、DECIMAL、TIMESTAMP |
| --- | --- |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |

## 六、脚本示例

见项目内`chunjun-examples`文件夹。
