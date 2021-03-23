# Emqx Writer
<!-- TOC -->

- [Emqx Writer](#emqx-writer)
    - [一、插件名称](#一插件名称)
    - [二、支持的数据源版本](#二支持的数据源版本)
    - [三、参数说明<br />](#三参数说明br-)
    - [四、配置示例](#四配置示例)

<!-- /TOC -->

<br />

<a name="c6v6n"></a>
## 一、插件名称
名称：**emqxwriter**<br />
<a name="jVb3v"></a>
## 二、支持的数据源版本
**Emqx 4.0及以上**<br />
<a name="2lzA4"></a>
## 三、参数说明<br />

- **broker**
  - 描述：连接URL信息。
  - 必选：是
  - 字段类型：String
  - 默认值：无
<br />


- **topic**
  - 描述：订阅主题
  - 必选：是
  - 字段类型：String
  - 默认值：无
<br />


- **username**
  - 描述：认证用户名
  - 必选：否
  - 字段类型：String
  - 默认值：无
<br />


- **password**
  - 描述：认证密码
  - 必选：否
  - 字段类型：String
  - 默认值：无
<br />


- **isCleanSession**
  - 描述：是否清除session
    - false：MQTT服务器保存于客户端会话的的主题与确认位置
    - true：MQTT服务器不保存于客户端会话的的主题与确认位置
  - 必选：否
  - 字段类型：boolean
  - 默认值：true
<br />


- **qos**
  - 描述：服务质量
    - 0：AT_MOST_ONCE，至多一次；
    - 1：AT_LEAST_ONCE，至少一次；
    - 2：EXACTLY_ONCE，精准一次；
  - 必选：否
  - 字段类型：int
  - 默认值：2
<br />


<a name="1LBc2"></a>
## 四、配置示例
```json
{
  "job": {
    "content": [{
      "reader": {
        "name": "streamreader",
        "parameter": {
          "column": [
            {
              "name": "id",
              "type": "id"
            },
            {
              "name": "user_id",
              "type": "int"
            },
            {
              "name": "name",
              "type": "string"
            }
          ],
          "sliceRecordCount" : [ "100"]
        }
      },
      "writer": {
        "parameter" : {
          "broker" : "tcp://localhost:1883",
          "topic" : "test",
          "username" : "admin",
          "password" : "public",
          "isCleanSession": true,
          "qos": 2
        },
        "name" : "emqxwriter"
      }
    }
    ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 1
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "log" : {
        "isLogger": false,
        "level" : "debug",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```
<br />
