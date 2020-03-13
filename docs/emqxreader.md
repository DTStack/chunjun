# Emqx读取插件（emqxreader）

## 1. 配置样例

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter" : {
            "broker" : "tcp://impala2:1883",
            "topic" : "mqtt/test",
            "username" : "root",
            "password" : "abc123",
            "isCleanSession": true,
            "qos": 2,
            "codec": "plain"
          },
          "name" : "emqxreader"
        },
        "writer": {

        }
      }
    ],
    "setting": {
      "errorLimit": {
        "record": 1
      },
      "speed": {
        "bytes": 1048576,
        "channel": 1
      }
    }
  }
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名，此处填写插件名称，eqmxreader。
  
  * 必选：是 
  
  * 默认值：无 

* **broker**
  
  * 描述：连接URL信息。
  
  * 必选：是
  
  * 默认值：无

* **topic**
  
  * 描述：订阅主题
  
  * 必选：是
  
  * 默认值：无
  
* **username**
  
  * 描述：认证用户名
  
  * 必选：否
  
  * 默认值：无 
  
* **password**
  
  * 描述：认证密码
  
  * 必选：否
  
  * 默认值：无
  
* **isCleanSession**
  
  * 描述：是否清除session
  
        * false：MQTT服务器保存于客户端会话的的主题与确认位置
        
        * true：MQTT服务器不保存于客户端会话的的主题与确认位置
  
  * 必选：否
  
  * 默认值：true   
  
* **qos**
  
  * 描述：服务质量
  
    *   0：AT_MOST_ONCE，至多一次；
    
    *   1：AT_LEAST_ONCE，至少一次；
    
    *   2：EXACTLY_ONCE，精准一次；
  
  * 必选：否
  
  * 默认值：2     

* **codec**
  
  * 描述：编码解码器类型，支持 json、plain 
  
  * 必选：否
  
  * 默认值：plain 
