# ODPS Reader

<a name="c6v6n"></a>
## 一、插件名称
名称：**odpsreader**<br />
<a name="2lzA4"></a>
## 二、参数说明

- **accessId**
  - 描述：ODPS系统登录ID
  - 必选：是
  - 默认值：无



- **accessKey**
  - 描述：ODPS系统登录Key
  - 必选：是
  - 默认值：无



- **project**
  - 描述：读取数据表所在的 ODPS 项目名称（大小写不敏感）
  - 必选：是
  - 默认值：无



- **table**
  - 描述：读取数据表的表名称（大小写不敏感）
  - 必选：是
  - 默认值：无


<br />

- **partition**
  - 描述：读取数据所在的分区信息，支持linux shell通配符，包括 * 表示0个或多个字符，?代表任意一个字符。例如现在有分区表 test，其存在 pt=1,ds=hangzhou   pt=1,ds=shanghai   pt=2,ds=hangzhou   pt=2,ds=beijing 四个分区，如果你想读取 pt=1,ds=shanghai 这个分区的数据，那么你应该配置为: `"partition":["pt=1,ds=shanghai"]`； 如果你想读取 pt=1下的所有分区，那么你应该配置为: `"partition":["pt=1,ds=* "]`；如果你想读取整个 test 表的所有分区的数据，那么你应该配置为: `"partition":["pt=*,ds=*"]`
  - 必选：如果表为分区表，则必填。如果表为非分区表，则不能填写
  - 默认值：无


<br />

- **column**
  - 描述：需要读取的字段。
  - 格式：支持3中格式
<br />1.读取全部字段，如果字段数量很多，可以使用下面的写法：
```
"column":[*]
```

  - <br />2.只指定字段名称：
```
"column":["id","name"]
```

  - <br />3.指定具体信息：
```
"column": [{
    "name": "col",
    "type": "datetime",
    "format": "yyyy-MM-dd hh:mm:ss",
    "value": "value"
}]
```

  - 属性说明:
    - name：字段名称
    - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
    - format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
    - value：如果数据库里不存在指定的字段，则会把value的值作为常量列返回，如果指定的字段存在，当指定字段的值为null时，会以此value值作为默认值返回
  - 必选：是
  - 默认值：无



<a name="1Pix9"></a>
## 三、配置示例
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "name": "odpsreader",
        "parameter": {
          "odpsConfig": {
            "accessId": "${odps.accessId}",
            "accessKey": "${odps.accessKey}",
            "project": "${odps.project}"
          },
          "table": "tableTest",
          "partition": "pt='xxooxx'",
          "column": [{
            "name": "col1",
            "type": "string",
            "value":"xx",
            "format":"yyyy-MM-dd HH:mm:ss"

          }]
        }
      },
      "writer" : {
        "parameter" : {
          "print" : true
        },
        "name" : "streamwriter"
      }
    } ],
    "setting" : {
      "restore" : {
        "maxRowNumForCheckpoint" : 0,
        "isRestore" : false,
        "restoreColumnName" : "",
        "restoreColumnIndex" : 0
      },
      "errorLimit" : {
        "record" : 100
      },
      "speed" : {
        "bytes" : 0,
        "channel" : 1
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
