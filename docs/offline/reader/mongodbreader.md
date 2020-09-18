# MongoDB Reader

<a name="c6v6n"></a>
## 一、插件名称
名称：**mongodbreader**
<a name="jVb3v"></a>
## 二、支持的数据源版本
**MongoDB 3.4及以上**
<a name="2lzA4"></a>
## 三、参数说明<br />

- **url**
  - 描述：MongoDB数据库连接的URL字符串，详细请参考[MongoDB官方文档](https://docs.mongodb.com/manual/reference/connection-string/)
  - 必选：否
  - 默认值：无



- **hostPorts**
  - 描述：MongoDB的地址和端口，格式为 IP1:port，可填写多个地址，以英文逗号分隔
  - 必选：否
  - 默认值：无



- **username**
  - 描述：数据源的用户名
  - 必选：否
  - 默认值：无



- **password**
  - 描述：数据源指定用户名的密码
  - 必选：否
  - 默认值：无



- **database**
  - 描述：数据库名称
  - 必选：否
  - 默认值：无



- **collectionName**
  - 描述：集合名称
  - 必选：是
  - 默认值：无
  - <br />
- **fetchSize**
  - 描述：每次读取的数据条数，通过调整此参数来优化读取速率
  - 必选：否
  - 默认值：100



- **filter**
  - 描述：过滤条件，通过该配置型来限制返回 MongoDB 数据范围，语法请参考[MongoDB查询语法](https://docs.mongodb.com/manual/crud/#read-operations)
  - 必选：否
  - 默认值：无



- **column**
  - 描述：需要读取的字段。
  - 格式：支持3中格式
<br />1.读取全部字段，如果字段数量很多，可以使用下面的写法：
```json
{"column":["*"]}
```
2.只指定字段名称：
```
"column":["id","name"]
```
3.指定具体信息：
```
"column": [{
    "name": "col",
    "type": "datetime",
    "format": "yyyy-MM-dd hh:mm:ss",
    "value": "value",
    "splitter":","
}]
```

  - 属性说明:
    - name：字段名称
    - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
    - format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
    - value：如果数据库里不存在指定的字段，则会把value的值作为常量列返回，如果指定的字段存在，当指定字段的值为null时，会以此value值作为默认值返回
    - splitter：因为 MongoDB 支持数组类型，所以 MongoDB 读出来的数组类型要通过这个分隔符合并成字符串
  - 必选：是
  - 默认值：无



<a name="1LBc2"></a>
## 四、配置示例
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "url": "mongodb://root:admin@kudu4:27017/admin?authSource=admin",
          "fetchSize": 100,
          "collectionName" : "tudou",
          "filter" : "{}",
          "column" : ["*"]
        },
        "name" : "mongodbreader"
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
