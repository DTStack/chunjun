# InfluxDB Sink

## 一、介绍

influxdb sink，不支持断点续传，只能根据time排序，而time非主键不唯一，如果你批量写入数据点，并且所有的写入点没有显示的 timestamp，那么它们将被以相同的 timestamp 写入。

## 二、支持版本

influxDB 1.x


## 三、插件名称

| Sync | influxdbsink、influxdbwriter |
| ---- |-----------------------------|
| SQL  |                             |


## 四、参数说明

### 1、Sync

- **connection**

    - 描述：数据库连接参数，包含jdbcUrl、database、measurement等参数

    - 必选：是

    - 参数类型：List

    - 默认值：无

      ```text
      "connection": [{
       "url": ["http://127.0.0.1:8086"],
       "measurement": ["measurement"],
       "database":"public"
      }]
      ```

       <br />

- **url**

    - 描述：连接influxDB的url
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **database**

    - 描述：数据库名
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **measurement**

    - 描述：目的表的表名称。目前只支持配置单个表，后续会支持多表
    - 必选：是
    - 参数类型：List
    - 默认值：无
      <br />

- **username**

    - 描述：数据源的用户名
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **password**

    - 描述：数据源指定用户名的密码
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **rp**

    - 描述：influxdb中的数据保留多长时间、数据保留几个副本（开源版只能保留一个）、每个shard保留多长时间的数据，创建一个retentionPolicy的要素：
        - DURATION：这个描述了保留策略要保留多久的数据。这个机制对于时序型的数据来讲，是非常有用的。
        - SHARD：这个是实际存储influxdb数据的单元。每个shard保留一个时间片的数据，默认是7天。如果你保存1年的数据，那么influxdb会把连续7天的数据放到一个shard中，使用好多个shard来保存数据。
        - SHARD DURATION：这个描述了每个shard存放多数据的时间片是多大。默认7天。需要注意的是，当数据超出了保留策略后，influxdb并不是按照数据点的时间一点一点删除的，而是会删除整个shard group。
        - SHARD GROUP：顾名思义，这个一个shard group包含多个shard。对于开源版的influxdb，这个其实没有什么区别，可以简单理解为一个shard group只包含一个shard，但对于企业版的多节点集群模式来讲，一个shard group可以包含不同节点上的不同shard，这使得influxdb可以保存更多的数据。
        - SHARD REPLICATION：这个描述了每个shard有几个副本。对于开源版来讲，只支持单副本，对于企业版来讲，每个shard可以冗余存储，这样可以避免单点故障。
    - 必选：否
    - 参数类型：String
    - 默认值：influxdb默认retention policy
      <br />
  
- **writeMode**

    - 描述：由于influxdb优先考虑create和read数据的性能而不是update和delete，对现有数据的更新是罕见的事件，持续地更新永远不会发生。时间序列数据主要是永远不更新的新数据。因此我们只支持insert操作
    - 必选：否
    - 参数类型：String
    - 默认值：insert
      <br />

- **enableBatch**

    - 描述：是否开启batch写入
    - 必选：否
    - 参数类型：boolean
    - 默认值：true
      <br />

- **bufferLimit**

    - 描述：批次，InfluxData建议每个buffer的大小在5000~10000个数据点
    - 必选：否
    - 参数类型：int
    - 默认值：10000
      <br />

- **flushDuration**

    - 描述：写入间隔，开启批次写入是从buffer flush进磁盘的间隔（毫秒）
    - 必选：否
    - 参数类型：int
    - 默认值：1000
      <br />

- **flushDuration**

    - 描述：与timestamp字段搭配使用，设置所提供的Unix时间的精度。如果您不指定精度，TSDB For InfluxDB®假设时间戳的精度为纳秒。
    - 必选：否
    - 可选值：ns,u,ms,s,m,h
    - 参数类型：string
    - 默认值：ns
      <br />

- **tag**

    - 描述：哪些字段是tag（measurement的tag对应的key）
    - 必选：否
    - 参数类型：list
    - 默认值：无

      ```text
      "tags":["id","no"],
      ```

       <br />


## 五、数据类型


|     是否支持     |                           类型名称                           |
|:---------------:| :----------------------------------------------------------: |
|       支持       | SMALLINT、BINARY_DOUBLE、CHAR、VARCHAR、VARCHAR2、NCHAR、NVARCHAR2、INT、INTEGER、NUMBER、DECIMAL、FLOAT、DATE、RAW、LONG RAW、BINARY_FLOAT、TIMESTAMP、TIMESTAMP WITH LOCAL TIME ZONE、TIMESTAMP WITH TIME ZON、INTERVAL YEAR、INTERVAL DAY |


## 六、脚本示例

见项目内`chunjun-examples`文件夹。

