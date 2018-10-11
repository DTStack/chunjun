# FlinkX

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

### 3.2 执行环境

* Java: JDK8及以上
* Flink集群: 1.4及以上（单机模式不需要安装Flink集群）
* 操作系统：理论上不限，但是目前只编写了shell启动脚本，用户可以可以参考shell脚本编写适合特定操作系统的启动脚本。


### 3.3 打包

进入项目根目录，使用maven打包：

```
mvn clean package -Dmaven.test.skip
```

打包结束后，项目根目录下会产生bin目录和plugins目录，其中bin目录包含FlinkX的启动脚本，plugins目录下存放编译好的数据同步插件包

### 3.4 启动

#### 3.4.1 命令行参数选项

* **model**
	* 描述：执行模式，也就是flink集群的工作模式
		* local: 本地模式
		* standalone: 独立部署模式的flink集群
		* yarn: yarn模式的flink集群
	* 必选：否
	* 默认值：local

* **job**
	* 描述：数据同步任务描述文件的存放路径；该描述文件中使用json字符串存放任务信息。
	* 必选：是
	* 默认值：无
	
* **plugin**
	* 描述：插件根目录地址，也就是打包后产生的plugins目录。
	* 必选：是
	* 默认值：无
	
* **flinkconf**
	* 描述：flink配置文件所在的目录（单机模式下不需要），如/hadoop/flink-1.4.0/conf
	* 必选：否
	* 默认值：无
	
* **yarnconf**
	* 描述：Hadoop配置文件（包括hdfs和yarn）所在的目录（单机模式下不需要），如/hadoop/etc/hadoop
	* 必选：否
	* 默认值：无

#### 3.4.2 启动数据同步任务
* **以本地模式启动数据同步任务**

```
bin/flinkx -mode local -job /Users/softfly/company/flink-data-transfer/jobs/task_to_run.json -plugin /Users/softfly/company/flink-data-transfer/plugins
```
* **以standalone模式启动数据同步任务**

```
bin/flinkx -mode standalone -job /Users/softfly/company/flink-data-transfer/jobs/oracle_to_oracle.json  -plugin /Users/softfly/company/flink-data-transfer/plugins -flinkconf /hadoop/flink-1.4.0/conf
```

* **以yarn模式启动数据同步任务**

```
bin/flinkx -mode yarn -job /Users/softfly/company/flinkx/jobs/mysql_to_mysql.json  -plugin /opt/dtstack/flinkplugin/syncplugin -flinkconf /opt/dtstack/myconf/conf -yarnconf /opt/dtstack/myconf/hadoop
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
		"dirty": {...}
	}
```
setting包括speed、errorLimit和dirty三部分，分别描述限速、错误控制和脏数据管理的配置信息

#### 4.1.1 speed

```
            "speed": {
                 "channel": 3,
                 "bytes": 0
            }
```

* channel: 任务并发数
* bytes: 每秒字节数，默认为0（不限速）

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

* [MySQL读取插件](docs/mysqlreader.md)
* [MySQL分库分表读取插件](docs/mysqldreader.md)
* [Oracle读取插件](docs/oraclereader.md)
* [SQLServer读取插件](docs/sqlserverreader.md)
* [HDFS读取插件](docs/hdfsreader.md)
* [HBase读取插件](docs/hbasereader.md)
* [Elasticsearch读取插件](docs/esreader.md)
* [Ftp读取插件](docs/ftpreader.md)
* [Odps读取插件](docs/odpsreader.md)
* [PostgreSQL读取插件](docs/postgresqlreader.md)
* [MongoDB读取插件](docs/mongodbreader.md)

### 5.2 写入插件

* [MySQL写入插件](docs/mysqlwriter.md)
* [Oracle写入插件](docs/oraclewriter.md)
* [SQLServer写入插件](docs/sqlserverwriter.md)
* [HDFS写入插件](docs/hdfswriter.md)
* [HBase写入插件](docs/hbasewriter.md)
* [Elasticsearch写入插件](docs/eswriter.md)
* [Ftp写入插件](docs/ftpwriter.md)
* [Odps写入插件](docs/odpswriter.md)
* [PostgreSQL写入插件](docs/postgresqlwriter.md)
* [MongoDB写入插件](docs/mongodbwriter.md)
* [Redis写入插件](docs/rediswriter.md)

## 6.版本说明
 1.flinkx的分支版本跟flink的版本对应，比如：flinkx v1.4.0 对应 flink1.4.0,现在支持flink1.4和1.5
 

## 7.招聘信息
 1.大数据平台开发工程师，想了解岗位详细信息可以添加本人微信号ysqwhiletrue,注明招聘，如有意者发送简历至sishu@dtstack.com。
  








