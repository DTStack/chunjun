# Kafka写入插件（**writer）

## 1. 配置样例

```json
{
  "job": {
    "content": [
      {
        "reader": {

        },
        "writer": {
          "parameter": {
            "timezone" : "",

            "encoding" : "utf-8",

            "producerSettings" : {

              "zookeeper.connect" : "127.0.0.1:2181/kafka"

            },
            "topic" : "mufeng_est",

            "brokerList" : "172.16.8.107:9092"

          },
          "name": "kafka09writer"
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
  
  * 描述：插件名，目前支持版本09、10、11，名称分别为 kafka09writer、kafka10writer、kafka11writer。
  
  * 必选：是 
  
  * 默认值：无 

* **topic**
  
  * 描述：topic。
  
  * 必选：是
  
  * 默认值：无

* **encoding**
  
  * 描述：编码
  
  * 必选：否
  
  * 默认值：utf-8

* **brokerList**
  
  * 描述：kafka broker地址列表 
  
  * 必选：是
  
  * 默认值：无

* **timezone**
  
  * 描述：时区
  
  * 必选：是
  
  * 默认值：无

* **producerSettings**
  
  * 描述：kafka连接配置
  
  * 必选：是
  
  * 默认值：无
