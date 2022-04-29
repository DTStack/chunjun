# Emqx Sink

## 一、介绍

emqx sink

## 二、支持版本

主流版本

## 三、插件名称

| Sync | emqxsink、emqxwriter |
| --- | --- |
| SQL | emqx-x |

## 四、参数说明

### 1、Sync

- **broker**
    - 描述：连接URL信息
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **topic**
    - 描述：订阅主题
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **username**
    - 描述：认证用户名
    - 必选：否
    - 参数类型：string
    - 默认值：无
      <br />

- **password**
    - 描述：认证密码
    - 必选：否
    - 参数类型：string
    - 默认值：无
      <br />

- **isCleanSession**
    - 描述：是否清除session
        - false：MQTT服务器保存于客户端会话的的主题与确认位置；
        - true：MQTT服务器不保存于客户端会话的的主题与确认位置
    - 必选：否
    - 参数类型：boolean
    - 默认值：true
      <br />

- **qos**
    - 描述：服务质量
        - 0：AT_MOST_ONCE，至多一次；
        - 1：AT_LEAST_ONCE，至少一次；
        - 2：EXACTLY_ONCE，精准一次；
    - 必选：否
    - 参数类型：int
    - 默认值：2
      <br />

### 2、SQL

- **connector**
    - 描述：emqx-x
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **broker**
    - 描述：连接信息tcp://localhost:1883
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **topic**
    - 描述：主题
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **isCleanSession**
    - 描述：是否清除session
        - false：MQTT服务器保存于客户端会话的的主题与确认位置；
        - true：MQTT服务器不保存于客户端会话的的主题与确认位置
    - 必选：否
    - 参数类型：String
    - 默认值：true
      <br />

- **qos**
    - 描述：服务质量
        - 0：AT_MOST_ONCE，至多一次；
        - 1：AT_LEAST_ONCE，至少一次；
        - 2：EXACTLY_ONCE，精准一次；
    - 必选：否
    - 参数类型：String
    - 默认值：2
      <br />

- **username**
    - 描述：username
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **password**
    - 描述：password
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

## 五、数据类型

| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY、ARRAY、MAP、STRUCT、LIST、ROW |
| --- | --- |
| 暂不支持 | 其他 |

## 六、脚本示例

见项目内`flinkx-examples`文件夹。
