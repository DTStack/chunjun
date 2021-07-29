# MongoDB Writer

<a name="c6v6n"></a>
## 一、插件名称
名称：**mongodbwriter**<br />
<a name="jVb3v"></a>
## 二、支持的数据源版本
**MongoDB 3.4及以上**<br />
<a name="2lzA4"></a>
## 三、参数说明<br />

- **url**
  - 描述：MongoDB数据库连接的URL字符串，详细请参考[MongoDB官方文档](https://docs.mongodb.com/manual/reference/connection-string/)
  - 必选：否
  - 默认值：无



- **hostPorts**
  - 描述：MongoDB的地址和端口，格式为 IP1:port，可填写多个地址，以英文逗号分隔
  - 必选：是
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



- **column**
  - 描述：MongoDB 的文档列名，配置为数组形式表示 MongoDB 的多个列
    - name：Column 的名字
    - type：Column 的类型
    - splitter：特殊分隔符，当且仅当要处理的字符串要用分隔符分隔为字符数组 Array 时，才使用这个参数。通过这个参数指定的分隔符，将字符串分隔存储到 MongoDB 的数组中
  - 必选：是
  - 默认值：无



- **replaceKey**
  - 描述：replaceKey 指定了每行记录的业务主键，用来做覆盖时使用（不支持 replaceKey为多个键，一般是指Monogo中的主键）
  - 必选：否
  - 默认值：无



- **writeMode**
  - 描述：写入模式，当 batchSize > 1 时不支持 replace 和 update 模式
  - 必选：是
  - 所有选项：insert/replace/update
  - 默认值：insert



- **batchSize**
  - 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与MongoDB的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况
  - 必选：否
  - 默认值：1



<a name="1LBc2"></a>
## 四、配置示例
<a name="ok5tA"></a>
#### 1、insert
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "column" : [ {
            "name" : "terminal_id",
            "type" : "id"
          }, {
            "name" : "longitude",
            "type" : "double"
          }, {
            "name" : "latitude",
            "type" : "double"
          }, {
            "name" : "created_at",
            "type" : "date"
          } ],
          "sliceRecordCount" : [ "100"]
        },
        "name" : "streamreader"
      },
      "writer" : {
        "parameter" : {
          "url": "mongodb://root:admin@kudu4:27017/admin?authSource=admin",
          "collectionName" : "tudou",
          "column" : [ {
            "name" : "terminal_id",
            "type" : "int"
          }, {
            "name" : "longitude",
            "type" : "double"
          }, {
            "name" : "latitude",
            "type" : "double"
          }, {
            "name" : "created_at",
            "type" : "date"
          } ],
          "writeMode": "insert",
          "batchSize": 100
        },
        "name" : "mongodbwriter"
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
<a name="HDscN"></a>
#### 2、update
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "column" : [ {
            "name" : "terminal_id",
            "type" : "id"
          }, {
            "name" : "longitude",
            "type" : "double",
            "value": 1.0
          }, {
            "name" : "latitude",
            "type" : "double",
            "value": 2.0
          }, {
            "name" : "created_at",
            "type" : "date"
          } ],
          "sliceRecordCount" : [ "100"]
        },
        "name" : "streamreader"
      },
      "writer" : {
        "parameter" : {
          "url": "mongodb://root:admin@kudu4:27017/admin?authSource=admin",
          "collectionName" : "tudou",
          "column" : [ {
            "name" : "terminal_id",
            "type" : "int"
          }, {
            "name" : "longitude",
            "type" : "double"
          }, {
            "name" : "latitude",
            "type" : "double"
          }, {
            "name" : "created_at",
            "type" : "date"
          } ],
          "writeMode": "update",
          "replaceKey": "terminal_id",
          "batchSize": 1
        },
        "name" : "mongodbwriter"
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
<a name="cFa8a"></a>
#### 3、replace
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "column" : [ {
            "name" : "terminal_id",
            "type" : "id"
          }, {
            "name" : "longitude",
            "type" : "double",
            "value": 1.0
          }, {
            "name" : "latitude",
            "type" : "double",
            "value": 3.0
          }, {
            "name" : "created_at",
            "type" : "date"
          } ],
          "sliceRecordCount" : [ "100"]
        },
        "name" : "streamreader"
      },
      "writer" : {
        "parameter" : {
          "url": "mongodb://root:admin@kudu4:27017/admin?authSource=admin",
          "collectionName" : "tudou",
          "column" : [ {
            "name" : "terminal_id",
            "type" : "int"
          }, {
            "name" : "longitude",
            "type" : "double"
          }, {
            "name" : "latitude",
            "type" : "double"
          }, {
            "name" : "created_at",
            "type" : "date"
          } ],
          "writeMode": "replace",
          "replaceKey": "terminal_id",
          "batchSize": 1
        },
        "name" : "mongodbwriter"
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
