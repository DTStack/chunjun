# Rabbitmq  Source

##  一、介绍

Rabbitmq source插件能够实时的从Rabbitmq server中读取数据。

##  二、支持版本

Rabbitmq 3.7

## 三、插件名称

| 插件类型 | 插件名称 |
| -------- | -------- |
| SQL      | rabbit-x |

##  四、参数说明

###  1、SQL

- **host**

    - 描述：rabbitmq service host
    - 必选：是
    - 字段类型：string
    - 默认值：无
      <br />

- **username**

    - 描述：rabbitmq 用户名
    - 必选：是
    - 字段类型：string
    - 默认值：无
      <br />

- **password**

    - 描述：rabbitmq指定用户名的密码
    - 必选：是
    - 字段类型：string
    - 默认值：无
      <br />

- **port**

    - 描述：rabbitmq server 端口
    - 必选：否
    - 字段类型：int
    - 默认值：5672
      <br />

- **virtual-host**

    - 描述：rabbitmq指定用户名对应的虚拟路径。
    - 必选：是
    - 字段类型：string
    - 默认值：无
      <br />
- **queue-name**

    - 描述：消费的队列名称
    - 必选：是
    - 字段类型：string
    - 默认值：无
      <br />

- **network-recovery-interval**

    - 描述：rabbitmq恢复连接的时间间隔(毫秒)
    - 必选：否
    - 字段类型：string
    - 默认值：无
      <br />

- **automatic-recovery**

    - 描述：rabbitmq 是否自动恢复连接
    - 必选：否
    - 字段类型：string
    - 默认值：无
      <br />

- **topology-recovery**

    - 描述：rabbitmq 是否启用拓扑恢复

    - 必选：否

    - 字段类型：boolean

    - 默认值：无

    - 描述：topology的恢复包括如下行为:

        - Re-declare exchange (exception for predefined ones)
        - Re-declare queues
        - Recover all bindings
        - Recover all consumers

      </br>

- **connection-timeout**

    - 描述：连接超时时间
    - 必选：否
    - 字段类型：int
    - 默认值：无
      <br />

- **requested-channel-max**

    - 描述：设置请求的最大通道数，该值必须介于 0 和 65535 之间
    - 必选：否
    - 字段类型：int
    - 默认值：无
      <br />

- **requested-frame-max**

    - 描述：设置请求的最大帧大小
    - 必选：否
    - 字段类型：int
    - 默认值：无
      <br />

- **requested-heartbeat**

    - 描述：设置请求的心跳超时。心跳帧将以大约 1/2 超时间隔发送。该值必须介于 0 和 65535 之间。
    - 必选：否
    - 字段类型：int
    - 默认值：无
      <br />

- **delivery-timeout**

    - 描述：如果队列没数据就等待指定时间，超时还没数据返回null。
    - 必选：否
    - 字段类型：long
    - 默认值：30000(毫秒)
      <br />

- **prefetch-count**

    - 描述：rabbitmq server 预取计数，该值必须介于 0 和 65535 之间。
    - 必选：否
    - 字段类型：int
    - 默认值：无
      <br />

- **use-correlationId**

    - 描述：是否为接收到的消息提供一个唯一的id，以消除消息重复(在确认失败的情况下)。仅在启用检查点时使用。
    - 必选：否
    - 字段类型：boolean
    - 默认值：true
      <br />

- **format**

    - 描述：参考flink [csv format](https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/connectors/formats/csv.html)与[json format](https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/connectors/formats/json.html)

##  六、数据结构

支持flink sql所有[数据类型](https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/types.html)


##  七、脚本示例

见项目内`chunjun-examples`文件夹。

