# Restapi Reader

<a name="4atlg"></a>
## 一、插件名称
名称：restapireader<br />

<a name="fSJWG"></a>
## 二、参数说明

- **url**
   - 描述：连接的url
   - 必选：是
   - 默认值：无



- **method**
   - 描述：request的类型，`post`、`get`
   - 必选：是
   - 默认值：无



- header
   - 描述：需要添加的报头信息
   - 必选：否
   - 默认值：无



<a name="IBBub"></a>
## 三、配置示例
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "url": "http://kudu3/server/index.php?g=Web&c=Mock&o=mock&projectID=58&uri=/api/tiezhu/test/get",
            "body": "",
            "method": "get",
            "params": ""
          }
        },
        "writer": {
          "parameter": {
            "print": true
          },
          "name": "streamwriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "isRestore": false,
        "isStream": true
      },
      "errorLimit": {},
      "speed": {
        "bytes": 0,
        "channel": 1
      },
      "log": {
        "isLogger": false,
        "level": "trace",
        "path": "",
        "pattern": ""
      }
    }
  }
}
```


