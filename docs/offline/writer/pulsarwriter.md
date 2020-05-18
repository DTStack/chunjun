# Pulsar写入插件（**writer）

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
             "producerSettings" : {
                "producerName":"test-producer"
              },
              "topic" : "pulsar_test",
              "pulsarServiceUrl" : "pulsar://127.0.0.1:6650"

          },
          "name": "pulsarwriter"
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
  
  * 描述：插件名，pulsarwriter。
  
  * 必选：是 
  
  * 默认值：无 

* **topic**
  
  * 描述：topic。
  
  * 必选：是
  
  * 默认值：无


* **pulsarServiceUrl**
  
  * 描述：pulsar地址列表 
  
  * 必选：是
  
  * 默认值：无



* **producerSettings**
  
  * 描述：pulsar生产者配置
  
  * 必选：是
  
  * 默认值：无

参考: https://pulsar.apache.org/docs/en/client-libraries-java/#configure-producer