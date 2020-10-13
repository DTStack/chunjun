# Stream Reader

<a name="hFWLG"></a>
## 一、简介
为了让用户能快速熟悉与使用，FlinkX提供了不需要数据库就能读取数据的Stream reader插件。<br />该插件利用了模拟数据的JMockData框架，能够根据给定的属性生成相应的随机数据，方便用户修改和调试。<br />

<a name="m8Adp"></a>
## 二、插件名称
名称：**streamreader**

<a name="DKB5u"></a>
### 三、参数说明

- **sliceRecordCount**
  - 描述：每个通道生成的数据条数，不配置此参数或者配置为0，程序会持续生成数据，不会停止
  - 必选：否
  - 默认值：0



- **column**
  - 描述：随机Java数据类型的字段信息
  - 格式：一组或多组描述"name"和"type"的json格式
  - 格式说明：
```json
{
  "name": "id",
  "type": "int",
  "value":"7"
}
```

  - "name"属性为用户提供的标识，类似于mysql的列名，必须填写。
  - "tpye" 属性为需要生成的数据类型，可配置以下类型：
    - id：从0开始步长为1的int类型自增ID
    - int，integer
    - byte
    - boolean
    - char，character
    - short
    - long
    - float
    - double
    - date
    - timestamp
    - bigdecimal
    - biginteger
    - int[]
    - byte[]
    - boolean[]
    - char[]，character[]
    - short[]
    - long[]
    - float[]
    - double[]
    - string[]
    - binary
    - string：以上均不匹配时默认为string字符串
  - "value"属性为用户设定的输出值，可以不填。



<a name="vja2R"></a>
## 四、使用实例
<a name="7SGOv"></a>
#### 1、单通道构造100条数据
```json
{
  "job": {
    "content": [
      {
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
            "sliceRecordCount" : ["100"]
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
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
<a name="6QR2o"></a>
#### 2、单通道无限构造数据
```json
{
  "job": {
    "content": [
      {
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
            "sliceRecordCount" : ["0"]
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
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
<a name="FljIK"></a>
#### 3、多通道构造数据，每个通道100条
```json
{
  "job": {
    "content": [
      {
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
            "sliceRecordCount" : ["100","100"]
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 2,
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
