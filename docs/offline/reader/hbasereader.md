# HBase Reader

<a name="CIuCt"></a>
## 一、插件名称
名称：**hbasereader**
<a name="jVb3v"></a>
## 二、支持的数据源版本
**HBase 1.2及以上**
<a name="mizE7"></a>
## 三、参数说明

- **table**
  - 描述：hbase表名
  - 必选：是
  - 字段类型：String
  - 默认值：无
  <br />

- **hbaseConfig**
  - 描述：hbase的连接配置，以json的形式组织 (见hbase-site.xml)
    - 基础配置  
     ```
         "hbase.zookeeper.property.clientPort": "2181",
         "hbase.rootdir": "hdfs://ns1/hbase",
         "hbase.cluster.distributed": "true",
         "hbase.zookeeper.quorum": "node01,node02,node03",
         "zookeeper.znode.parent": "/hbase"
    ```

    - kerberos配置
    在hbaseConfig中加入以下三条中的任一条即表明开启Kerberos配置：  
    ```
        "hbase.security.authentication" ："Kerberos",
        "hbase.security.authorization" : "Kerberos",
        "hbase.security.auth.enable" : true
    ```
    在开启kerberos后，需要根据自己的集群指定以下两个principal的value值  
    ```
        "hbase.regionserver.kerberos.principal":"hbase/_HOST@DTSTACK.COM",
        "hbase.master.kerberos.principal":"hbase/_HOST@DTSTACK.COM"
    ```
    还需要指定Kerberos相关文件的位置  
    ```
        "principalFile": "path of keytab",
        "java.security.krb5.conf": "path of krb5.conf"
    ```
  - 必选：是
  - 字段类型：Map
  - 默认值：无

  <br />
  
- **range**
  - 描述：指定hbasereader读取的rowkey范围。
    - startRowkey：指定开始rowkey；
    - endRowkey：指定结束rowkey；
    - isBinaryRowkey：指定配置的startRowkey和endRowkey转换为byte[]时的方式，默认值为false。
    若为true，则调用Bytes.toBytesBinary(rowkey)方法进行转换;若为false：则调用Bytes.toBytes(rowkey)  
    配置格式如下：
    ```
    "range": {
     "startRowkey": "aaa",
     "endRowkey": "ccc",
     "isBinaryRowkey":false
    }
    ```
  - 注意：如果用户配置了 startRowkey 和 endRowkey，需要确保：startRowkey <= endRowkey
  - 必选：否
  - 字段类型：Map
  - 默认值：无
  
  <br />

- **encoding**
  - 描述：字符编码
  - 必选：无
  - 字段类型：String
  - 默认值：UTF-8
  
  <br />

- **scanCacheSize**
  - 描述：一次RPC请求批量读取的Results数量。cache值得设置并不是越大越好，需要做一个平衡。cache的值越大，则查询的性能就越高，但是与此同时，每一次调用next（）操作都需要花费更长的时间，因为获取的数据更多并且数据量大了传输到客户端需要的时间就越长，一旦超过了maximum heap the client process 拥有的值，就会报outofmemoryException异常。当传输rows数据到客户端的时候，如果花费时间过长，则会抛出ScannerTimeOutException异常。
  - 必选：无
  - 字段类型：String
  - 默认值：256
  
  <br />

- **column**
  - 描述：要读取的hbase字段
    - name：指定读取的hbase列，除了rowkey外，必须为 列族:列名 的格式，注意rowkey区分大小写；
    - type：指定源数据的类型，format指定日期类型的格式，value指定当前类型为常量，不从hbase读取数据，而是根据value值自动生成对应的列。
  - 必选：是
  - 字段类型：List
  - 默认值：无
  
  <br />

<br />

<a name="kQbcJ"></a>
## 四、配置示例
未开启Kerberos的情况
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

开启kerberos的情况
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
              "zookeeper.znode.parent": "/hbase",
              "hbase.security.auth.enable": true,
              "hbase.regionserver.kerberos.principal":"hbase/host@DTSTACK.COM",
              "hbase.master.kerberos.principal":"hbase/host@DTSTACK.COM",
              "principalFile": "path of keytab",
              "useLocalFile": "true",
              "java.security.krb5.conf": "path of krb5.conf"
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
              "isBinaryRowkey": false
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
