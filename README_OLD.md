## 技术交流
  * 招聘大数据平台开发工程师，想了解岗位详细信息可以添加本人微信号ysqwhiletrue,注明招聘，如有意者发送简历至sishu@dtstack.com
  
  * 可以搜索群号30537511或者扫描下面的二维码进入钉钉群
   <div align=center>
     <img src=docs/images/ding.jpg width=300 />
   </div>


## 1 什么是FlinkX

* **FlinkX是在是袋鼠云内部广泛使用的基于flink的分布式离线数据同步框架，实现了多种异构数据源之间高效的数据迁移。**

不同的数据源头被抽象成不同的Reader插件，不同的数据目标被抽象成不同的Writer插件。理论上，FlinkX框架可以支持任意数据源类型的数据同步工作。作为一套生态系统，每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。

<div align=center>
    <img src=docs/images/template.png width=400 />
</div>

## 2 工作原理

在底层实现上，FlinkX依赖Flink，数据同步任务会被翻译成StreamGraph在Flink上执行，工作原理如下图：

<div align=center>
    <img src=docs/images/diagram.png width=600 />
</div>

## 3 快速起步

### 3.1 运行模式

* 单机模式：对应Flink集群的单机模式
* standalone模式：对应Flink集群的分布式模式
* yarn模式：对应Flink集群的yarn模式
* yarnPer模式: 对应Flink集群的Per-job模式


### 3.2 执行环境

* Java: JDK8及以上
* Flink集群: 1.4及以上（单机模式不需要安装Flink集群）
* 操作系统：理论上不限，但是目前只编写了shell启动脚本，用户可以可以参考shell脚本编写适合特定操作系统的启动脚本。

### 3.3 打包

进入项目根目录，使用maven打包：

windows平台
```
mvn clean package -DskipTests -DscriptType=bat
```
unix平台
```
mvn clean package -DskipTests -DscriptType=sh
```

打包结束后，项目根目录下会产生bin目录和plugins目录，其中bin目录包含FlinkX的启动脚本，plugins目录下存放编译好的数据同步插件包

### 3.4 启动

#### 3.4.1 命令行参数选项

* **model**
  
  * 描述：执行模式，也就是flink集群的工作模式
    * local: 本地模式
    * standalone: 独立部署模式的flink集群
    * yarn: yarn模式的flink集群，需要提前在yarn上启动一个flink session，使用默认名称"Flink session cluster"
    * yarnPer: yarn模式的flink集群，单独为当前任务启动一个flink session，使用默认名称"Flink per-job cluster"
  * 必选：否
  * 默认值：local

* **job**
  
  * 描述：数据同步任务描述文件的存放路径；该描述文件中使用json字符串存放任务信息。
  * 必选：是
  * 默认值：无

* **pluginRoot**
  
  * 描述：插件根目录地址，也就是打包后产生的pluginRoot目录。
  * 必选：是
  * 默认值：无

* **flinkconf**
  
  * 描述：flink配置文件所在的目录（单机模式下不需要），如/opt/dtstack/flink-1.8.1/conf
  * 必选：否
  * 默认值：无

* **yarnconf**
  
  * 描述：Hadoop配置文件（包括hdfs和yarn）所在的目录（单机模式下不需要），如/hadoop/etc/hadoop
  * 必选：否
  * 默认值：无
  
* **flinkLibJar**
  
  * 描述：flink lib所在的目录（单机模式下不需要），如/opt/dtstack/flink-1.8.1/lib
  * 必选：否
  * 默认值：无
  
* **confProp**
  
  * 描述：flink相关参数，如{\"flink.checkpoint.interval\":200000}
  * 必选：否
  * 默认值：无
     
* **queue**
  
  * 描述：yarn队列，如default
  * 必选：否
  * 默认值：无
  
* **pluginLoadMode**
  
  * 描述：yarnPer模式插件加载方式：
    * classpath：提交任务时不上传插件包，需要在yarn-node节点pluginRoot目录下部署插件包，但任务启动速度较快
    * shipfile：提交任务时上传pluginRoot目录下部署插件包的插件包，yarn-node节点不需要部署插件包，任务启动速度取决于插件包的大小及网络环境
  * 必选：否
  * 默认值：classpath        

#### 3.4.2 启动数据同步任务

* **以本地模式启动数据同步任务**

```
bin/flinkx -mode local \
            -job /Users/softfly/company/flink-data-transfer/jobs/task_to_run.json \
            -plugin /Users/softfly/company/flink-data-transfer/plugins \
            -confProp "{"flink.checkpoint.interval":60000,"flink.checkpoint.stateBackend":"/flink_checkpoint/"}" \
            -s /flink_checkpoint/0481473685a8e7d22e7bd079d6e5c08c/chk-*
```

* **以standalone模式启动数据同步任务**

```
bin/flinkx -mode standalone \
            -job /Users/softfly/company/flink-data-transfer/jobs/oracle_to_oracle.json \
            -plugin /Users/softfly/company/flink-data-transfer/plugins \
            -flinkconf /hadoop/flink-1.4.0/conf \
            -confProp "{"flink.checkpoint.interval":60000,"flink.checkpoint.stateBackend":"/flink_checkpoint/"}" \
            -s /flink_checkpoint/0481473685a8e7d22e7bd079d6e5c08c/chk-*
```

* **以yarn模式启动数据同步任务**

```
bin/flinkx -mode yarn \
            -job /Users/softfly/company/flinkx/jobs/mysql_to_mysql.json \
            -plugin /opt/dtstack/flinkplugin/syncplugin \
            -flinkconf /opt/dtstack/myconf/conf \
            -yarnconf /opt/dtstack/myconf/hadoop \
            -confProp "{"flink.checkpoint.interval":60000,"flink.checkpoint.stateBackend":"/flink_checkpoint/"}" \
            -s /flink_checkpoint/0481473685a8e7d22e7bd079d6e5c08c/chk-*
```

* **以perjob模式启动数据同步任务**

```
bin/flinkx -mode yarnPer \
            -job /test.json \
            -pluginRoot /opt/dtstack/syncplugin \
            -flinkconf /opt/dtstack/flink-1.8.1/conf \
            -yarnconf /opt/dtstack/hadoop-2.7.3/etc/hadoop \
            -flinkLibJar /opt/dtstack/flink-1.8.1/lib \
            -confProp {\"flink.checkpoint.interval\":200000} \
            -queue c -pluginLoadMode classpath
```

## 4 数据同步任务模版

从最高空俯视，一个数据同步的构成很简单，如下：

```
{
    "job": {
        "setting": {...},
        "content": [...]
    }
}
```

数据同步任务包括一个job元素，而这个元素包括setting和content两部分。

* setting: 用于配置限速、错误控制和脏数据管理
* content: 用于配置具体任务信息，包括从哪里来（Reader插件信息），到哪里去（Writer插件信息）

### 4.1 setting

```
    "setting": {
        "speed": {...},
        "errorLimit": {...},
        "dirty": {...},
        "restart": {...}
    }
```

setting包括speed、errorLimit和dirty三部分，分别描述限速、错误控制和脏数据管理的配置信息

#### 4.1.1 speed

```
"speed": {
    "bytes": 1048576,
    "channel": 2,
    "rebalance": false,
    "readerChannel": 1,
    "writerChannel": 1
}
```

* channel：任务并发数
* readerChannel：reader的并发数，配置此参数时会覆盖channel配置的并发数，不配置或配置为-1时将使用channel配置的并发数作为reader的并发数。
* writerChannel：writer的并发数，配置此参数时会覆盖channel配置的并发数，不配置或配置为-1时将使用channel配置的并发数作为writer的并发数。
* rebalance：此参数配置为true时将强制对reader的数据做Rebalance，不配置此参数或者配置为false时，程序会根据reader和writer的通道数选择是否Rebalance，reader和writer的通道数一致时不使用Reblance，通道数不一致时使用Reblance。
* bytes:：每秒字节数，默认为 Long.MAX_VALUE

#### 4.1.2 errorLimit

```
            "errorLimit": {
                "record": 10000,
                "percentage": 100
            }
```

* record: 出错记录数超过record设置的条数时，任务标记为失败
* percentage: 当出错记录数超过percentage百分数时，任务标记为失败

#### 4.1.3 dirty

```
        "dirty": {
                "path": "/tmp",
                "hadoopConfig": {
                    "fs.default.name": "hdfs://ns1",
                    "dfs.nameservices": "ns1",
                    "dfs.ha.namenodes.ns1": "nn1,nn2",
                    "dfs.namenode.rpc-address.ns1.nn1": "node02:9000",
                    "dfs.namenode.rpc-address.ns1.nn2": "node03:9000",
                    "dfs.ha.automatic-failover.enabled": "true",
                    "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                    "fs.hdfs.impl.disable.cache": "true"
                }
            }
```

* path: 脏数据存放路径
* hadoopConfig: 脏数据存放路径对应hdfs的配置信息(hdfs高可用配置)

#### 4.1.4  restore

```
"restore": {

        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      }
```

restore配置请参考[断点续传](docs/restore.md)

#### 4.1.5  log

```
"log" : {
        "isLogger": true,
        "level" : "warn",
        "path" : "/opt/log/",
        "pattern":""
      }
```
* isLogger: 日志是否保存到磁盘, `true`: 是; `false`(默认): 否;
* level: 日志输出级别, `trace`, `debug`, `info`(默认), `warn`, `error`;
* path: 日志保存路径, 默认为`/tmp/dtstack/flinkx/`, 日志名称为当前flink任务的jobID，如: `97501729f8c44c260d889d099968cc74.log`
* pattern: 日志输出格式
    * log4j默认格式为: `%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{60} %X{sourceThread} - %msg%n`; 
    * logback默认格式为: `%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n`

注意：该日志记录功能只会记录`com.dtstack`包下的输出日志, 如需变更，可修改类参数`DtLogger.LOGGER_NAME`。

#### 4.1.6  restart

```
"restart": {
        "strategy": "fixedDelay",
        "restartAttempts": 5,
        "delayInterval": 10,
        "failureRate":2,
        "failureInterval":60
      }
```
* strategy：重启策略，可选：NoRestart、fixedDelay、failureRate，可参考[Flink文档](https://ci.apache.org/projects/flink/flink-docs-stable/dev/task_failure_recovery.html)

### 4.2 content

```
        "content": [
            {
               "reader": {
                    "name": "...",
                    "parameter": {
                        ...
                    }
                },
               "writer": {
                    "name": "...",
                    "parameter": {
                         ...
                     }
                }
            }
        ]
```

* reader: 用于读取数据的插件的信息
* writer: 用于写入数据的插件的信息

reader和writer包括name和parameter，分别表示插件名称和插件参数

### 4.3 数据同步任务例子

详见flinkx-examples子工程

## 5. 数据同步插件

### 5.1 读取插件

* [关系数据库读取插件(Mysql,Oracle,Sqlserver,Postgresql,Db2,Gbase,SAP Hana,Teradata,Phoenix,达梦)](docs/rdbreader.md)
* [分库分表读取插件](docs/rdbdreader.md)
* [HDFS读取插件](docs/hdfsreader.md)
* [HBase读取插件](docs/hbasereader.md)
* [Elasticsearch读取插件](docs/esreader.md)
* [Ftp读取插件](docs/ftpreader.md)
* [Odps读取插件](docs/odpsreader.md)
* [MongoDB读取插件](docs/mongodbreader.md)
* [Stream读取插件](docs/streamreader.md)
* [Carbondata读取插件](docs/carbondatareader.md)
* [MySQL binlog读取插件](docs/binlog.md)
* [KafKa读取插件](docs/kafkareader.md)
* [Kudu读取插件](docs/kudureader.md)
* [Cassandra读取插件](docs/cassandrareader.md)
* [Emqx读取插件](docs/emqxreader.md)
* [MongoDB实时采集插件](docs/mongodb_oplog.md)
* [PostgreSQL WAL实时采集插件](docs/pgwalreader.md)

### 5.2 写入插件

* [关系数据库写入插件(Mysql,Oracle,Sqlserver,Postgresql,Db2,Gbase,SAP Hana,Teradata,Phoenix)](docs/rdbwriter.md)
* [HDFS写入插件](docs/hdfswriter.md)
* [HBase写入插件](docs/hbasewriter.md)
* [Elasticsearch写入插件](docs/eswriter.md)
* [Ftp写入插件](docs/ftpwriter.md)
* [Odps写入插件](docs/odpswriter.md)
* [MongoDB写入插件](docs/mongodbwriter.md)
* [Redis写入插件](docs/rediswriter.md)
* [Stream写入插件](docs/streamwriter.md)
* [Carbondata写入插件](docs/carbondatawriter.md)
* [Kafka写入插件](docs/kafkawriter.md)
* [Hive写入插件](docs/hivewriter.md)
* [Kudu写入插件](docs/kuduwriter.md)
* [Cassandra写入插件](docs/cassandrawriter.md)
* [Emqx写入插件](docs/emqxwriter.md)

[断点续传和实时采集功能介绍](docs/restore.md)

[数据源开启Kerberos](docs/kerberos.md)

[统计指标说明](docs/statistics.md)

## 6.版本说明

 1.flinkx的分支版本跟flink的版本对应，比如：flinkx v1.5.0 对应 flink1.5.0,版本说明：

| 插件版本  | flink版本 |
| ----- | ------- |
| 1.5.x | 1.5.4   |
| 1.8.x | 1.8.1   |
