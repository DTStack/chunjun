# Restapi Writer

<a name="CGka5"></a>
## 一、插件名称
**名称：restapiwriter**<br />
<a name="8qehQ"></a>
## 二、参数说明

- **url**
   - 描述：连接的url
   - 必选：是
   - 默认值：无



- **method**
   - 描述：request的类型，`post`、`get`
   - 必选：是
   - 默认值：无



- **header**
   - 描述：需要添加的报头信息
   - 必选：否
   - 默认值：无



- **body**
   - 描述：发送的数据中包括params
   - 必选：否
   - 默认值：无



- **params**
   - 描述：发送的数据中包括params
   - 必选：否
   - 默认值：无



- **column**
   - 描述：如果column不为空，那么将数据和字段名一一对应。如果column为空，则返回每个数据的第一个字段。
   - 必选：否
   - 默认值：无



<a name="cjQlX"></a>
## 三、使用示例
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "column": [
              {
                "name": "id",
                "type": "id"
              },
              {
                "name": "data",
                "type": "string"
              }
            ],
            "sliceRecordCount": [
              "100"
            ]
          },
          "name": "streamreader"
        },
        "writer": {
          "parameter": {
            "url": "http://kudu3/server/index.php?g=Web&c=Mock&o=mock&projectID=58&uri=/api/tiezhu/test/get",
            "header": [],
            "body": [],
            "method": "post",
            "params": {},
            "column": ["id","data"]
          },
          "name": "restapiwriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "isStream": true,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "errorLimit": {
        "record": 100
      },
      "speed": {
        "bytes": 0,
        "channel": 1
      },
      "log": {
        "isLogger": false,
        "level": "debug",
        "path": "",
        "pattern": ""
      }
    }
  }
}
```


