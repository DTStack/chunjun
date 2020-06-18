# HBase Reader

<a name="CIuCt"></a>
## 一、插件名称
名称：**hbasereader**
<a name="jVb3v"></a>
## 二、支持的数据源版本
**HBase 1.3及以上**
<a name="mizE7"></a>
## 三、参数说明

- **table**
  - 描述：hbase表名
  - 必选：是
  - 默认值：无



- **hbaseConfig**
  - 描述：hbase的连接配置，以json的形式组织 (见hbase-site.xml)，key可以为以下七种：

Kerberos；<br />hbase.security.authentication；<br />hbase.security.authorization；<br />hbase.master.kerberos.principal；<br />hbase.master.keytab.file；<br />hbase.regionserver.keytab.file；<br />hbase.regionserver.kerberos.principal

  - 必选：是
  - 默认值：无



- **range**
  - 描述：指定hbasereader读取的rowkey范围。
    - startRowkey：指定开始rowkey；
    - endRowkey：指定结束rowkey；


    - isBinaryRowkey：指定配置的startRowkey和endRowkey转换为byte[]时的方式，默认值为false,若为true，则调用Bytes.toBytesBinary(rowkey)方法进行转换;若为false：则调用Bytes.toBytes(rowkey)，配置格式如下：
```
"range": {
 "startRowkey": "aaa",
 "endRowkey": "ccc",
 "isBinaryRowkey":false
}
```


  - 注意：如果用户配置了 startRowkey 和 endRowkey，需要确保：startRowkey <= endRowkey
  - 必选：否
  - 默认值：无



- **encoding**
  - 描述：字符编码
  - 必选：无
  - 默认值：无



- **scanCacheSize**
  - 描述：一次RPC请求批量读取的Results数量
  - 必选：无
  - 默认值：256

<br />

- **scanBatchSize**
  - 描述：每一个result中的列的数量
  - 必选：无
  - 默认值：100

<br />

- **column**
  - 描述：要读取的hbase字段，normal 模式与multiVersionFixedColumn 模式下必填项。
    - name：指定读取的hbase列，除了rowkey外，必须为 列族:列名 的格式；
    - type：指定源数据的类型，format指定日期类型的格式，value指定当前类型为常量，不从hbase读取数据，而是根据value值自动生成对应的列。
  - 必选：是
  - 默认值：无


<br />

<a name="kQbcJ"></a>
## 四、配置示例
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "hbasereader",
          "parameter": {
            "hbaseConfig": {
              "hbase.zookeeper.property.clientPort": "2181",
              "hbase.rootdir": "hdfs://ns1/hbase",
              "hbase.cluster.distributed": "true",
              "hbase.zookeeper.quorum": "node01,node02,node03",
              "zookeeper.znode.parent": "/hbase"
            },
            "table": "sb5",
            "encodig": "utf-8",
            "column": [
              {
                "name": "rowkey",
                "type": "string"
              },
              {
                "name": "cf1:id",
                "type": "string"
              }
            ],
            "range": {
              "startRowkey": "",
              "endRowkey": "",
              "isBinaryRowkey": true
            }
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
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "isStream": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
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
<a name="HBOGY"></a>
# 
