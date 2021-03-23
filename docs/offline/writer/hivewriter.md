# Hive Writer

## 一、插件名称
名称：**hivewriter**

## 二、支持的数据源版本
**Hive 2.X**

## 三、参数说明


- **jdbcUrl**
  - 描述：连接Hive JDBC的字符串
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **username**
  - 描述：Hive认证用户名
  - 必选：否
  - 字段类型：string
  - 默认值：无

<br/>

- **password**
  - 描述：Hive认证密码
  - 必选：否
  - 字段类型：string
  - 默认值：无

<br/>

- **fileType**
  - 描述：文件的类型，目前只支持用户配置为`text`、`orc`、`parquet`
    - text：textfile文件格式
    - orc：orcfile文件格式
    - parquet：parquet文件格式
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **fieldDelimiter**
  - 描述：hivewriter中`fileType`为`text`时字段的分隔符，
  - 注意：用户需要保证与创建的Hive表的字段分隔符一致，否则无法在Hive表中查到数据
  - 必选：否
  - 字段类型：string
  - 默认值：`\u0001`

<br/>

- **writeMode**
  - 描述：hivewriter写入前数据清理处理模式：
    - append：追加
    - overwrite：覆盖
  - 注意：overwrite模式时会删除Hive当前分区下的所有文件
  - 必选：否
  - 字段类型：string
  - 默认值：append

<br/>

- **compress**
  - 描述：hdfs文件压缩类型，默认不填写意味着没有压缩
    - text：支持`GZIP`、`BZIP2`格式
    - orc：支持`SNAPPY`、`GZIP`、`BZIP`、`LZ4`格式
    - parquet：支持`SNAPPY`、`GZIP`、`LZO`
  - 注意：`SNAPPY`格式需要用户安装**SnappyCodec**
  - 必选：否
  - 字段类型：string
  - 默认值：
    - text 默认 不进行压缩
    - orc 默认为ZLIB格式
    - parquet 默认为SNAPPY格式

<br/>


- **charsetName**
  - 描述：写入text文件的编码配置
  - 必选：否
  - 字段类型：string
  - 默认值：UTF-8

<br/>

- **maxFileSize**
  - 描述：写入hdfs单个文件最大大小，单位字节
  - 必须：否
  - 字段类型：string
  - 默认值：1073741824‬（1G）

<br/>

- **tablesColumn**
  - 描述：写入hive表的表结构信息，**若表不存在则会自动建表**。
  - 必选：是
  - 字段类型：json
  - 默认值：无
  - 示例：
```json
{
    "kudu":[
        {
            "key":"id",
            "type":"int"
        },
        {
            "key":"user_id",
            "type":"int"
        },
        {
            "key":"name",
            "type":"string"
        }
    ]
}
```

<br/>

- **distributeTable**
  - 描述：如果数据来源于各个CDC数据，则将不同的表进行聚合，多张表的数据写入同一个hive表
  - 必选：否
  - 字段类型：json
  - 默认值：无  
  - 示例：
```json
 "distributeTable" : "{\"fenzu1\":[\"table1\"],\"fenzu2\":[\"table2\",\"table3\"]}"
```
table1的数据将写入hive表fenzu1里，table2和table3的数据将写入fenzu2里,如果配置distributeTable，则tablesColumn需要配置为如下格式：
```json
{
    "fenzu1":[
        {
            "key":"id",
            "type":"int"
        },
        {
            "key":"user_id",
            "type":"int"
        },
        {
            "key":"name",
            "type":"string"
        }
    ],
   "fenzu2":[
        {
            "key":"id",
            "type":"int"
        },
        {
            "key":"user_id",
            "type":"int"
        },
        {
            "key":"name",
            "type":"string"
        }
    ]
}
```

<br/>

- **partition**
  - 描述：分区字段名称
  - 必选：否
  - 字段类型：string
  - 默认值：`pt`

<br/>

- **partitionType**
  - 描述：分区类型，包括 DAY、HOUR、MINUTE三种。**若分区不存在则会自动创建，自动创建的分区时间以当前任务运行的服务器时间为准**
    - DAY：天分区，分区示例：pt=20200101
    - HOUR：小时分区，分区示例：pt=2020010110
    - MINUTE：分钟分区，分区示例：pt=202001011027
  - 必选：否
  - 字段类型：string
  - 默认值：无

<br/>

- **defaultFS**
  - 描述：Hadoop hdfs文件系统namenode节点地址。取core-site.xml文件里fs.defaultFS配置值
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **hadoopConfig**
  - 描述：集群HA模式时需要填写的namespace配置及其它hive配置
  - 必选：否
  - 字段类型：map
  - 默认值：无

<br/>

- **rowGroupSIze**
  - 描述：parquet格式文件的row group的大小，单位字节
  - 必选：否
  - 字段类型：int
  - 默认值：134217728（128M）

<br/>

- **analyticalRules**
  - 描述： 建表的动态规则获取表名，按照${XXXX}的占位符，从待写入数据(map结构)里根据key XXX 获取值进行替换，创建对应的表，并将数据写入对应的表
  - 示例：stream_${schema}_${table}
  - 必选：否
  - 字段类型：string
  - 默认值：无

<br/>

- **schema**
  - 描述： 自动建表时，analyticalRules里如果指定schema占位符，schema将此schema参数值进行替换
  - 必选：否
  - 字段类型：string
  - 默认值：无



## 四、配置示例
#### 1、写入text
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "name": "id",
                "type": "id"
              },
              {
                "name": "user_id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              }
            ],
            "sliceRecordCount": ["100"]
          }
        },
        "writer": {
          "name" : "hivewriter",
          "parameter" : {
            "jdbcUrl" : "jdbc:hive2://kudu3:10000/tudou",
            "username" : "",
            "password" : "",
            "fileType" : "text",
            "fieldDelimiter" : "\u0001",
            "writeMode" : "overwrite",
            "compress" : "",
            "charsetName" : "UTF-8",
            "maxFileSize" : 1073741824,
            "tablesColumn" : "{\"kudu\":[{\"key\":\"id\",\"type\":\"int\"},{\"key\":\"user_id\",\"type\":\"int\"},{\"key\":\"name\",\"type\":\"string\"}]}",
            "partition" : "pt",
            "partitionType" : "DAY",
            "defaultFS" : "hdfs://ns1",
            "hadoopConfig" : {
            "javax.jdo.option.ConnectionDriverName": "com.mysql.jdbc.Driver",
            "dfs.replication": "2",
            "dfs.ha.fencing.ssh.private-key-files": "~/.ssh/id_rsa",
            "dfs.nameservices": "ns1",
            "fs.hdfs.impl.disable.cache": "true",
            "dfs.safemode.threshold.pct": "0.5",
            "dfs.ha.namenodes.ns1": "nn1,nn2",
            "dfs.journalnode.rpc-address": "0.0.0.0:8485",
            "dfs.journalnode.http-address": "0.0.0.0:8480",
            "dfs.namenode.rpc-address.ns1.nn2": "kudu2new:9000",
            "dfs.namenode.rpc-address.ns1.nn1": "kudu1new:9000",
            "hive.metastore.warehouse.dir": "/user/hive/warehouse",
            "hive.server2.webui.host": "172.16.10.34",
            "hive.metastore.schema.verification": "false",
            "hive.server2.support.dynamic.service.discovery": "true",
            "javax.jdo.option.ConnectionPassword": "abc123",
            "hive.metastore.uris": "thrift://kudu1new:9083",
            "hive.exec.dynamic.partition.mode": "nonstrict",
            "hadoop.proxyuser.admin.hosts": "*",
            "hive.zookeeper.quorum": "kudu1new:2181,kudu2new:2181,kudu3new:2181",
            "ha.zookeeper.quorum": "kudu1new:2181,kudu2new:2181,kudu3new:2181",
            "hive.server2.thrift.min.worker.threads": "200",
            "hive.server2.webui.port": "10002",
            "fs.defaultFS": "hdfs://ns1",
            "hadoop.proxyuser.admin.groups": "*",
            "dfs.ha.fencing.methods": "sshfence",
            "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
            "typeName": "yarn2-hdfs2-hadoop2",
            "hadoop.proxyuser.root.groups": "*",
            "javax.jdo.option.ConnectionURL": "jdbc:mysql://kudu2new:3306/ide?useSSL=false",
            "dfs.qjournal.write-txns.timeout.ms": "60000",
            "fs.trash.interval": "30",
            "hadoop.proxyuser.root.hosts": "*",
            "dfs.namenode.shared.edits.dir": "qjournal://kudu1new:8485;kudu2new:8485;kudu3new:8485/namenode-ha-data",
            "javax.jdo.option.ConnectionUserName": "dtstack",
            "hive.server2.thrift.port": "10000",
            "fs.hdfs.impl": "org.apache.hadoop.hdfs.DistributedFileSystem",
            "ha.zookeeper.session-timeout.ms": "5000",
            "hadoop.tmp.dir": "/data/hadoop_root",
            "dfs.journalnode.edits.dir": "/data/dtstack/hadoop/journal",
            "hive.server2.zookeeper.namespace": "hiveserver2",
            "hive.server2.enable.doAs": "/false",
            "dfs.namenode.http-address.ns1.nn2": "kudu2new:50070",
            "dfs.namenode.http-address.ns1.nn1": "kudu1new:50070",
            "hive.exec.scratchdir": "/user/hive/warehouse",
            "hive.server2.webui.max.threads": "100",
            "datanucleus.schema.autoCreateAll": "true",
            "hive.exec.dynamic.partition": "true",
            "hive.server2.thrift.bind.host": "kudu1",
            "dfs.ha.automatic-failover.enabled": "true"
            }
          }
        }
      }
    ],
    "setting": {
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0
      },
      "speed": {
        "bytes": 1048576,
        "channel": 1
      }
    }
  }
}
```
#### 2、写入orc
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "name": "id",
                "type": "id"
              },
              {
                "name": "user_id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              }
            ],
            "sliceRecordCount": ["100"]
          }
        },
        "writer": {
          "name" : "hivewriter",
          "parameter" : {
            "jdbcUrl" : "jdbc:hive2://kudu3:10000/tudou",
            "username" : "",
            "password" : "",
            "fileType" : "orc",
            "fieldDelimiter" : "",
            "writeMode" : "overwrite",
            "compress" : "GZIP",
            "charsetName" : "UTF-8",
            "maxFileSize" : 1073741824,
            "tablesColumn" : "{\"kudu\":[{\"key\":\"id\",\"type\":\"int\"},{\"key\":\"user_id\",\"type\":\"int\"},{\"key\":\"name\",\"type\":\"string\"}]}",
            "partition" : "pt",
            "partitionType" : "DAY",
            "defaultFS" : "hdfs://ns1",
            "hadoopConfig" : {
            "javax.jdo.option.ConnectionDriverName": "com.mysql.jdbc.Driver",
            "dfs.replication": "2",
            "dfs.ha.fencing.ssh.private-key-files": "~/.ssh/id_rsa",
            "dfs.nameservices": "ns1",
            "fs.hdfs.impl.disable.cache": "true",
            "dfs.safemode.threshold.pct": "0.5",
            "dfs.ha.namenodes.ns1": "nn1,nn2",
            "dfs.journalnode.rpc-address": "0.0.0.0:8485",
            "dfs.journalnode.http-address": "0.0.0.0:8480",
            "dfs.namenode.rpc-address.ns1.nn2": "kudu2new:9000",
            "dfs.namenode.rpc-address.ns1.nn1": "kudu1new:9000",
            "hive.metastore.warehouse.dir": "/user/hive/warehouse",
            "hive.server2.webui.host": "172.16.10.34",
            "hive.metastore.schema.verification": "false",
            "hive.server2.support.dynamic.service.discovery": "true",
            "javax.jdo.option.ConnectionPassword": "abc123",
            "hive.metastore.uris": "thrift://kudu1new:9083",
            "hive.exec.dynamic.partition.mode": "nonstrict",
            "hadoop.proxyuser.admin.hosts": "*",
            "hive.zookeeper.quorum": "kudu1new:2181,kudu2new:2181,kudu3new:2181",
            "ha.zookeeper.quorum": "kudu1new:2181,kudu2new:2181,kudu3new:2181",
            "hive.server2.thrift.min.worker.threads": "200",
            "hive.server2.webui.port": "10002",
            "fs.defaultFS": "hdfs://ns1",
            "hadoop.proxyuser.admin.groups": "*",
            "dfs.ha.fencing.methods": "sshfence",
            "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
            "typeName": "yarn2-hdfs2-hadoop2",
            "hadoop.proxyuser.root.groups": "*",
            "javax.jdo.option.ConnectionURL": "jdbc:mysql://kudu2new:3306/ide?useSSL=false",
            "dfs.qjournal.write-txns.timeout.ms": "60000",
            "fs.trash.interval": "30",
            "hadoop.proxyuser.root.hosts": "*",
            "dfs.namenode.shared.edits.dir": "qjournal://kudu1new:8485;kudu2new:8485;kudu3new:8485/namenode-ha-data",
            "javax.jdo.option.ConnectionUserName": "dtstack",
            "hive.server2.thrift.port": "10000",
            "fs.hdfs.impl": "org.apache.hadoop.hdfs.DistributedFileSystem",
            "ha.zookeeper.session-timeout.ms": "5000",
            "hadoop.tmp.dir": "/data/hadoop_root",
            "dfs.journalnode.edits.dir": "/data/dtstack/hadoop/journal",
            "hive.server2.zookeeper.namespace": "hiveserver2",
            "hive.server2.enable.doAs": "/false",
            "dfs.namenode.http-address.ns1.nn2": "kudu2new:50070",
            "dfs.namenode.http-address.ns1.nn1": "kudu1new:50070",
            "hive.exec.scratchdir": "/user/hive/warehouse",
            "hive.server2.webui.max.threads": "100",
            "datanucleus.schema.autoCreateAll": "true",
            "hive.exec.dynamic.partition": "true",
            "hive.server2.thrift.bind.host": "kudu1",
            "dfs.ha.automatic-failover.enabled": "true"
            }
          }
        }
      }
    ],
    "setting": {
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0
      },
      "speed": {
        "bytes": 1048576,
        "channel": 1
      }
    }
  }
}
```
#### 3、写入parquet
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "name": "id",
                "type": "id"
              },
              {
                "name": "user_id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              }
            ],
            "sliceRecordCount": ["100"]
          }
        },
        "writer": {
          "name" : "hivewriter",
          "parameter" : {
            "jdbcUrl" : "jdbc:hive2://kudu3:10000/tudou",
            "username" : "",
            "password" : "",
            "fileType" : "parquet",
            "fieldDelimiter" : "",
            "writeMode" : "overwrite",
            "compress" : "SNAPPY",
            "charsetName" : "UTF-8",
            "maxFileSize" : 1073741824,
            "tablesColumn" : "{\"kudu\":[{\"key\":\"id\",\"type\":\"int\"},{\"key\":\"user_id\",\"type\":\"int\"},{\"key\":\"name\",\"type\":\"string\"}]}",
            "partition" : "pt",
            "partitionType" : "DAY",
            "defaultFS" : "hdfs://ns1",
            "hadoopConfig" : {
              "javax.jdo.option.ConnectionDriverName": "com.mysql.jdbc.Driver",
              "dfs.replication": "2",
              "dfs.ha.fencing.ssh.private-key-files": "~/.ssh/id_rsa",
              "dfs.nameservices": "ns1",
              "fs.hdfs.impl.disable.cache": "true",
              "dfs.safemode.threshold.pct": "0.5",
              "dfs.ha.namenodes.ns1": "nn1,nn2",
              "dfs.journalnode.rpc-address": "0.0.0.0:8485",
              "dfs.journalnode.http-address": "0.0.0.0:8480",
              "dfs.namenode.rpc-address.ns1.nn2": "kudu2new:9000",
              "dfs.namenode.rpc-address.ns1.nn1": "kudu1new:9000",
              "hive.metastore.warehouse.dir": "/user/hive/warehouse",
              "hive.server2.webui.host": "172.16.10.34",
              "hive.metastore.schema.verification": "false",
              "hive.server2.support.dynamic.service.discovery": "true",
              "javax.jdo.option.ConnectionPassword": "abc123",
              "hive.metastore.uris": "thrift://kudu1new:9083",
              "hive.exec.dynamic.partition.mode": "nonstrict",
              "hadoop.proxyuser.admin.hosts": "*",
              "hive.zookeeper.quorum": "kudu1new:2181,kudu2new:2181,kudu3new:2181",
              "ha.zookeeper.quorum": "kudu1new:2181,kudu2new:2181,kudu3new:2181",
              "hive.server2.thrift.min.worker.threads": "200",
              "hive.server2.webui.port": "10002",
              "fs.defaultFS": "hdfs://ns1",
              "hadoop.proxyuser.admin.groups": "*",
              "dfs.ha.fencing.methods": "sshfence",
              "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "typeName": "yarn2-hdfs2-hadoop2",
              "hadoop.proxyuser.root.groups": "*",
              "javax.jdo.option.ConnectionURL": "jdbc:mysql://kudu2new:3306/ide?useSSL=false",
              "dfs.qjournal.write-txns.timeout.ms": "60000",
              "fs.trash.interval": "30",
              "hadoop.proxyuser.root.hosts": "*",
              "dfs.namenode.shared.edits.dir": "qjournal://kudu1new:8485;kudu2new:8485;kudu3new:8485/namenode-ha-data",
              "javax.jdo.option.ConnectionUserName": "dtstack",
              "hive.server2.thrift.port": "10000",
              "fs.hdfs.impl": "org.apache.hadoop.hdfs.DistributedFileSystem",
              "ha.zookeeper.session-timeout.ms": "5000",
              "hadoop.tmp.dir": "/data/hadoop_root",
              "dfs.journalnode.edits.dir": "/data/dtstack/hadoop/journal",
              "hive.server2.zookeeper.namespace": "hiveserver2",
              "hive.server2.enable.doAs": "/false",
              "dfs.namenode.http-address.ns1.nn2": "kudu2new:50070",
              "dfs.namenode.http-address.ns1.nn1": "kudu1new:50070",
              "hive.exec.scratchdir": "/user/hive/warehouse",
              "hive.server2.webui.max.threads": "100",
              "datanucleus.schema.autoCreateAll": "true",
              "hive.exec.dynamic.partition": "true",
              "hive.server2.thrift.bind.host": "kudu1",
              "dfs.ha.automatic-failover.enabled": "true"
            }
          }
        }
      }
    ],
    "setting": {
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0
      },
      "speed": {
        "bytes": 1048576,
        "channel": 1
      }
    }
  }
}
```
