## 一、介绍

读取Cassandra数据

## 二、支持版本

Cassandra主流版本

## 三、插件名称

| Sync | Cassandrasource、Cassandrareader |
| --- | --- |
| SQL | Cassandra-x |

## 四、参数说明

#### 1、数据同步

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

- **column**
    - 描述：需要读取的字段。
    - 属性说明:
        - name：字段名称
        - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
    - 必选：是
    - 字段类型：List
    - 默认值：

#### 2、SQL

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
      

## 五、数据类型

| 支持 | BYTE、INT、FLOAT、DOUBLE、BOOLEAN、TEXT、VARCHAR、DECIMAL、TIMESTAMP |
| --- | --- |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION


## 六、脚本示例

见项目内`chunjun-examples`文件夹。
