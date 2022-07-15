# RocketMQ Source

## 一、介绍
支持读取消息队列RocketMQ的数据; 支持解析的消息格式：JSON

## 二、支持的版本
RocketMQ 4.4+

## 三、插件名称
| sql | rocketmq-x |
| --- | --- |

## 四、参数说明

#### 1、sql

- **connector**
    - 描述：rocketmq-x
    - 必选：是
    - 字段类型：String
    - 默认值：无
      

- **topic**
    - 描述：需要读取的topic
    - 必选：是
    - 参数类型：String
    - 默认值：无
      

- **consumer.group**
    - 描述：消费者组
    - 必选：是
    - 字段类型：String
    - 默认值：无
      

- **nameserver.address**
    - 描述：集群nameserver地址，名称服务器为客户端，包括生产者，消费者和命令行客户端提供最新的路由信息。多个nameserver地址之间用分号分割。
    - 必选：是
    - 字段类型：String
    - 默认值：无
      

- **tag**
    - 描述：消息标签，方便服务器过滤使用。多个tag之间用'|'分隔
    - 必选：否
    - 字段类型：String
    - 默认值：*
      

- **access.key**
    - 描述：access.key，数据源开启acl后需要
    - 必选：否
    - 字段类型：String
    - 默认值：无
      

- **secret.key**
    - 描述：secret.key，数据源开启acl后需要
    - 必选：否
    - 字段类型：String
    - 默认值：无
      

- **access.channel**
    - 描述：对于阿里云的数据源实例，需要设置这个参数
    - 必选：否
    - 参数类型：string
    - 默认值：LOCAL
      

- **consumer.batch-size**
    - 描述：批量消费，一次消费多少条消息
    - 必选：否
    - 字段类型：Integer
    - 默认值：32
      

- **consumer.start-offset-mode**
    - 描述：consumer消费方式，可选值：
      - earliest：从最早的偏移量开始消费
      - latest：从最新的偏移量开始消费
      - timestamp：从指定的时间戳开始消费，搭配start.message-timestamp参数使用
      - offset：从指定的偏移量开始消费，搭配start.message-offset参数使用
    - 必选：否
    - 字段类型：String
    - 默认值：latest


- **start.message-offset**
    - 描述：当consumer.start-offset-mode为offset时，为每个消息队列指定起始消费的偏移量
    - 必选：否
    - 字段类型：Long
    - 默认值：-1L


- **start.message-timestamp**
    - 描述：当consumer.start-offset-mode为timestamp时，为每个消息队列指定起始消费的时间戳
    - 必选：否
    - 字段类型：Long
    - 默认值：-1L


- **start.time.ms**
    - 描述：意同start.message-timestamp，当start.message-timestamp未指定时生效
    - 必选：否 
    - 参数类型：Long
    - 默认值：-1L


- **start.time**
    - 描述：意同start.message-timestamp，当start.message-timestamp、start.time.ms都未指定时生效，三者都未指定则以的当前系统时间为准。格式为yyyy-MM-dd HH:mm:ss的时间字符串
    - 必选：否
    - 参数类型：Long
    - 默认值：无


- **time.zone**
    - 描述：搭配start.time参数使用，设置start.time的时区
    - 必选：否
    - 参数类型：String
    - 默认值：GMT+8


- **encoding**
    - 描述：消息解码的字符集，consumer消费到的消息是二进制数组，以此字符集进行解码
    - 必选：否
    - 参数类型：String
    - 默认值：UTF-8


- **heartbeat.broker.interval**
    - 描述：向Broker发送心跳间隔时间，单位毫秒
    - 必选：否
    - 参数类型：Integer
    - 默认值：30000


- **persist.consumer-offset-interval**
    - 描述：持久化Consumer消费进度间隔时间，单位毫秒
    - 必选：否
    - 参数类型：Integer
    - 默认值：5000


## 五、数据类型

| 是否支持 | 数据类型 |
| --- | ---|
| 支持 |  CHAR、VARCHAR、INT、BINARY、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、DATE、TIME、TIMESTAMP|
| 不支持 |  |


## 六、配置示例
见项目内`flinkx-examples`文件夹
