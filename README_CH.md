FlinkX
============

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

[English](README.md) | 中文

# 技术交流

- 招聘**大数据平台开发工程师**，想了解岗位详细信息可以添加本人微信号ysqwhiletrue，注明招聘，如有意者发送简历至[sishu@dtstack.com](mailto:sishu@dtstack.com)

- 我们使用[钉钉](https://www.dingtalk.com/)沟通交流，可以搜索群号[**30537511**]或者扫描下面的二维码进入钉钉群
  
  <div align=center>
     <img src=docs/images/ding.jpg width=300 />
   </div>

# 介绍

FlinkX是一个基于Flink的批流统一的数据同步工具，既可以采集静态的数据，比如MySQL，HDFS等，也可以采集实时变化的数据，比如MySQL binlog，Kafka等。FlinkX目前包含下面这些特性：

- 大部分插件支持并发读写数据，可以大幅度提高读写速度；

- 部分插件支持失败恢复的功能，可以从失败的位置恢复任务，节约运行时间；[失败恢复](docs/restore.md)

- 关系数据库的Reader插件支持间隔轮询功能，可以持续不断的采集变化的数据；[间隔轮询](docs/rdbreader.md)

- 部分数据库支持开启Kerberos安全认证；[Kerberos](docs/kerberos.md)

- 可以限制reader的读取速度，降低对业务数据库的影响；

- 可以记录writer插件写数据时产生的脏数据；

- 可以限制脏数据的最大数量；

- 支持多种运行模式；

FlinkX目前支持下面这些数据库：

|                        | Database Type  | Reader                          | Writer                          |
|:----------------------:|:--------------:|:-------------------------------:|:-------------------------------:|
| Batch Synchronization  | MySQL          | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | Oracle         | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | SqlServer      | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | PostgreSQL     | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | DB2            | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | GBase          | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | ClickHouse     | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | PolarDB        | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | SAP Hana       | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | Teradata       | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | Phoenix        | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | 达梦             | [doc](docs/rdbreader.md)        | [doc](docs/rdbwriter.md)        |
|                        | Cassandra      | [doc](docs/cassandrareader.md)  | [doc](docs/cassandrawriter.md)  |
|                        | ODPS           | [doc](docs/odpsreader.md)       | [doc](docs/odpswriter.md)       |
|                        | HBase          | [doc](docs/hbasereader.md)      | [doc](docs/hbasewriter.md)      |
|                        | MongoDB        | [doc](docs/mongodbreader.md)    | [doc](docs/mongodbwriter.md)    |
|                        | Kudu           | [doc](docs/kudureader.md)       | [doc](docs/kuduwriter.md)       |
|                        | ElasticSearch  | [doc](docs/esreader.md)         | [doc](docs/eswriter.md)         |
|                        | FTP            | [doc](docs/ftpreader.md)        | [doc](docs/ftpwriter.md)        |
|                        | HDFS           | [doc](docs/hdfsreader.md)       | [doc](docs/hdfswriter.md)       |
|                        | Carbondata     | [doc](docs/carbondatareader.md) | [doc](docs/carbondatawriter.md) |
|                        | Redis          |                                 | [doc](docs/rediswriter.md)      |
|                        | Hive           |                                 | [doc](docs/hivewriter.md)       |
| Stream Synchronization | Kafka          | [doc](docs/kafkareader.md)      | [doc](docs/kafkawriter.md)      |
|                        | EMQX           | [doc](docs/emqxreader.md)       | [doc](docs/emqxwriter.md)       |
|                        | MySQL Binlog   | [doc](docs/binlog.md)           |                                 |
|                        | MongoDB Oplog  | [doc](docs/mongodb_oplog.md)    |                                 |
|                        | PostgreSQL WAL | [doc](docs/pgwalreader.md)      |                                 |

# 参考文档

[参考文档](https://github.com/DTStack/flinkx/wiki) | [旧文档](README_OLD.md)

# License

FlinkX is under the Apache 2.0 license. See the [LICENSE](http://www.apache.org/licenses/LICENSE-2.0) file for details.
