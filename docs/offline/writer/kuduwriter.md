# Kudu Writer

<a name="c6v6n"></a>
## 一、插件名称
名称：**kuduwriter**<br />
<a name="jVb3v"></a>
## 二、支持的数据源版本
**kudu 1.10及以上**<br />

<a name="2lzA4"></a>
## 三、参数说明

- **column**
  - 描述：需要生成的字段
  - 属性说明:
    - name：字段名称；
    - type：字段类型；
  - 必选：是
  - 默认值：无



- **masterAddresses**
  - 描述： master节点地址:端口，多个以,隔开
  - 必选：是
  - 默认值：无



- **table**
  - 描述： kudu表名
  - 必选：是
  - 默认值：无



- **writeMode**
  - 描述： kudu数据写入模式：
    - 1、insert
    - 2、update
    - 3、upsert
  - 必选：是
  - 默认值：无



- **flushMode**
  - 描述： kudu session刷新模式：
    - 1、auto_flush_sync
    - 2、auto_flush_background
    - 3、manual_flush
  - 必选：否
  - 默认值：auto_flush_sync



- **batchInterval**
  - 描述： 单次批量写入数据条数
  - 必选：否
  - 默认值：1



- **authentication**
  - 描述： 认证方式，如:Kerberos
  - 必选：否
  - 默认值：无



- **principal**
  - 描述： 用户名。
  - 必选：否
  - 默认值：无



- **keytabFile**
  - 描述： keytab文件路径
  - 必选：否
  - 默认值：无



- **workerCount**
  - 描述： worker线程数
  - 必选：否
  - 默认值：默认为cpu*2



- **bossCount**
  - 描述： boss线程数
  - 必选：否
  - 默认值：1



- **operationTimeout**
  - 描述： 普通操作超时时间
  - 必选：否
  - 默认值：30000



- **adminOperationTimeout**
  - 描述： 管理员操作(建表，删表)超时时间
  - 必选：否
  - 默认值：30000



- **queryTimeout**
  - 描述： 连接scan token的超时时间
  - 必选：否
  - 默认值：与operationTimeout一致



- **batchSizeBytes**
  - 描述： kudu scan一次性最大读取字节数
  - 必选：否
  - 默认值：1048576



<a name="1Pix9"></a>
## 四、配置示例
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "column" : [ {
            "name" : "id",
            "type" : "id"
          }, {
            "name" : "data",
            "type" : "string"
          } ],
          "sliceRecordCount" : [ "100"]
        },
        "name" : "streamreader"
      },
      "writer" : {
        "parameter": {
          "column": [
            {
              "name": "id",
              "type": "long"
            }
          ],
          "masterAddresses": "kudu1:7051,kudu2:7051,kudu3:7051",
          "table": "kudu",
          "writeMode": "insert",
          "flushMode": "manual_flush",
          "batchInterval": 10000,
          "authentication": "",
          "principal": "",
          "keytabFile": "",
          "workerCount": 2,
          "bossCount": 1,
          "operationTimeout": 30000,
          "adminOperationTimeout": 30000,
          "queryTimeout": 30000,
          "batchSizeBytes": 1048576
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
