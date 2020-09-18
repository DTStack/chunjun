# Stream Writer

<a name="LB4vl"></a>
## 一、 简介
Stream writer插件仅仅用来测试reader插件的读取效果，处理方式为简单的将读到的数据弃掉，可选择是否通过LOG显示读取到的数据<br />

<a name="c6v6n"></a>
## 二、插件名称
名称：**streamwriter**<br />
<a name="VAhCe"></a>
## 三、 参数说明

- **print**
  - 描述：boolean值，表明是否通过LOG INFO打印采集到的数据信息
  - 必选：否
  - 默认值：false



<a name="BG1bt"></a>
## 四、 配置示例
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
            "sliceRecordCount" : [ "100"]
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


