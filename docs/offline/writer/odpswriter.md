# ODPS Writer

<a name="c6v6n"></a>
## 一、插件名称
名称：**odpswriter**<br />
<a name="2lzA4"></a>
## 二、参数说明

- **odpsConfig**
  - 描述：ODPS系统配置参数，包含以下参数
    - accessId：ODPS系统登录ID
    - accessKey：ODPS系统登录Key
    - project：读取数据表所在的 ODPS 项目名称（大小写不敏感）
    - packageAuthorizedProject：ODPS认证项目，不填默认为project值
    - accountType：ODPS账户类型，默认为aliyun
    - odpsServer：ODPS服务URL，默认为[http://service.odps.aliyun.com/api](http://service.odps.aliyun.com/api)
  - 必选：是
  - 默认值：无



- **table**
  - 描述：读取数据表的表名称（大小写不敏感）
  - 必选：是
  - 默认值：无


<br />

- **partition**
  - 描述：需要写入数据表的分区信息，必须指定到最后一级分区。把数据写入一个三级分区表，必须配置到最后一级分区，例如pt=20150101/type＝1/biz=2。
  - 必选：**如果是分区表，该选项必填，如果非分区表，该选项不可填写。**
  - 默认值：空


<br />

- **column**
  - 描述：需要导入的字段列表，当导入全部字段时，可以配置为"column": ["*"], 当需要插入部分odps列填写部分列，例如"column": ["id", "name"]。ODPSWriter支持列筛选、列换序，例如表有a,b,c三个字段，用户只同步c,b两个字段。可以配置成["c","b"], 在导入过程中，字段a自动补空，设置为null。
  - 必选：否
  - 默认值：无


<br />

- **writeMode**
  - 描述：写入模式，支持append和overwrite
  - 必填：否
  - 默认值：append



- **bufferSize**
  - 描述：写入缓存大小，单位兆，odps写入数据时会先缓存，达到一定值后才会写入数据，如果写入数据时出现内存溢出，可以降低此参数的值。
  - 必填：否
  - 默认值：64



<a name="1Pix9"></a>
## 三、配置示例
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "column" : [ {
            "name" : "data",
            "type" : "string"
          } ],
          "sliceRecordCount" : [ "100"]
        },
        "name" : "streamreader"
      },
      "writer" : {
        "name": "odpswriter",
        "parameter": {
          "odpsConfig": {
            "accessId": "${odps.accessId}",
            "accessKey": "${odps.accessKey}",
            "project": "${odps.project}"
          },
          "table": "tableTest",
          "partition": "pt='xx'",
          "writeMode": "append",
          "bufferSize": 64,
          "column": [{
            "name": "col1",
            "type": "string"
          }]
        }
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
