# Kafka读取插件（**reader）

## 1. 配置样例

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "topic" : "yxabc",
            "codec" : "plain",
            "encoding" : "utf-8",
            "consumerSettings" : {
              "zookeeper.connect" : "127.0.0.1:2181/kafka",
              "group.id" : "default",
              "auto.commit.interval.ms" : "1000",
              "auto.offset.reset" : "smallest"
            }
          },
          "name": "kafka09reader"
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
  
  * 描述：插件名，目前支持版本09、10、11，名称分别为 kafka09reader、kafka10reader、kafka11reader。
  
  * 必选：是 
  
  * 默认值：无 

* **topic**
  
  * 描述：要消费的topic。
  
  * 必选：是
  
  * 默认值：无

* **encoding**
  
  * 描述：编码
  
  * 必选：否
  
  * 默认值：utf-8

* **codec**
  
  * 描述：编码解码器类型，支持 json、plain 
  
  * 必选：否
  
  * 默认值：plain 

* **consumerSettings**
  
  * 描述：kafka连接配置
  
  * 必选：是
  
  * 默认值：无
