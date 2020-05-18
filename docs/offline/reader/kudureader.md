# Kudu Reader

<a name="c6v6n"></a>
## 一、插件名称
名称：**kudureader**<br />**
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
  - 描述： kudu表名。
  - 必选：是
  - 默认值：无



- **readMode**
  - 描述： kudu读取模式：
    - 1、read_latest<br />
默认的读取模式。<br />
该模式下，服务器将始终在收到请求时返回已提交的写操作。<br />
这种类型的读取不会返回快照时间戳，并且不可重复。<br />
用ACID术语表示，它对应于隔离模式：“读已提交”。
    - 2、read_at_snapshot<br />
该模式下，服务器将尝试在提供的时间戳上执行读取。<br />
如果未提供时间戳，则服务器将当前时间作为快照时间戳。<br />
在这种模式下，读取是可重复的，即将来所有在相同时间戳记下的读取将产生相同的数据。<br />
执行此操作的代价是等待时间戳小于快照的时间戳的正在进行的正在进行的事务，因此可能会导致延迟损失。用ACID术语，这本身就相当于隔离模式“可重复读取”。<br />
如果对已扫描tablet的所有写入均在外部保持一致，则这对应于隔离模式“严格可序列化”。<br />
注意：当前存在“空洞”，在罕见的边缘条件下会发生，通过这种空洞有时即使在采取措施使写入如此时，它们在外部也不一致。<br />
在这些情况下，隔离可能会退化为“读取已提交”模式。
  - 必选：是
  - 默认值：无



- **authentication**
  - 描述：认证方式，如:Kerberos
  - 必选：否
  - 默认值：无



- **principal**
  - 描述： 用户名
  - 必选：否
  - 默认值：无



- **keytabFile**
  - 描述：keytab文件路径
  - 必选：否
  - 默认值：无



- **workerCount**
  - 描述：worker线程数
  - 必选：否
  - 默认值：默认为cpu*2



- **bossCount**
  - 描述：boss线程数
  - 必选：否
  - 默认值：1



- **operationTimeout**
  - 描述：普通操作超时时间
  - 必选：否
  - 默认值：30000



- **adminOperationTimeout**
  - 描述： 管理员操作(建表，删表)超时时间
  - 必选：否
  - 默认值：30000



- **queryTimeout**
  - 描述：连接scan token的超时时间
  - 必选：否
  - 默认值：与operationTimeout一致



- **where**
  - 描述：过滤条件字符串，多个以and连接
  - 必选：否
  - 默认值：无



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
        "name": "kudureader",
        "parameter": {
          "column": [
            {
              "name": "id",
              "type": "long"
            }
          ],
          "masterAddresses": "kudu1:7051,kudu2:7051,kudu3:7051",
          "table": "kudu",
          "readMode": "read_latest",
          "authentication": "",
          "principal": "",
          "keytabFile": "",
          "workerCount": 2,
          "bossCount": 1,
          "operationTimeout": 30000,
          "adminOperationTimeout": 30000,
          "queryTimeout": 30000,
          "where": " id >= 1 ",
          "batchSizeBytes": 1048576
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
