Chunjun
============

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

[English](README.md) | 中文

# 技术交流

- 招聘**Flink研发工程师**，如果有兴趣可以联系思枢（微信号：ysqwhiletrue）<BR>
  Flink开发工程师JD要求：<BR>
  1.负责袋鼠云基于Flink的衍生框架数据同步chunjun和实时计算flinkstreamsql框架的开发；<BR>
  2.调研和把握当前最新大数据实时计算技术，将其中的合适技术引入到平台中，改善产品，提升竞争力；<BR>
  职位要求：<BR>
  1、本科及以上学历，3年及以上的Flink开发经验，精通Java，熟悉Scala、Python优先考虑；<BR>
  2、熟悉Flink原理，有基于Flink做过二次源码的开发，在github上贡献者Flink源码者优先；<BR>
  3、有机器学习、数据挖掘相关经验者优先；<BR>
  4、对新技术有快速学习和上手能力，对代码有一定的洁癖；<BR>
  加分项：<BR>
  1.在GitHub或其他平台上有过开源项目<BR>
  可以添加本人微信号ysqwhiletrue，注明招聘，如有意者发送简历至[sishu@dtstack.com](mailto:sishu@dtstack.com)

- 我们使用[钉钉](https://www.dingtalk.com/)沟通交流，可以搜索群号[**30537511**]或者扫描下面的二维码进入钉钉群

  <div align=center>
     <img src=docs/images/ding.jpg width=300 />
   </div>

# 介绍

*[Chunjun 1.12 新特性](docs/changeLog.md)*

Chunjun是一个基于Flink的批流统一的数据同步工具，既可以采集静态的数据，比如MySQL，HDFS等，也可以采集实时变化的数据，比如MySQL binlog，Kafka等。**同时，Chunjun也是支持原生FlinkSql所有语法和特性的计算框架**，<big>**并且提供了大量[案例](Chunjun-examples)**</big>。Chunjun目前包含下面这些特性：

- 大部分插件支持并发读写数据，可以大幅度提高读写速度；

- 部分插件支持失败恢复的功能，可以从失败的位置恢复任务，节约运行时间；[失败恢复](docs/restore.md)

- 关系数据库的Source插件支持间隔轮询功能，可以持续不断的采集变化的数据；[间隔轮询](docs/offline/reader/mysqlreader.md)

- 部分数据库支持开启Kerberos安全认证；[Kerberos](docs/kerberos.md)

- 可以限制source的读取速度，降低对业务数据库的影响；

- 可以记录sink插件写数据时产生的脏数据；

- 可以限制脏数据的最大数量；

- 支持多种运行模式；

- **同步任务支持执行flinksql语法的transformer操作；**

- **sql任务支持和flinkSql自带connectors[共用](docs/conectorShare.md)；**

Chunjun目前支持下面这些数据库：

|                        | Database Type  | Source                                                    | Sink                                                      | Lookup
|:----------------------:|:--------------:|:---------------------------------------------------------:|:---------------------------------------------------------:|:---------------------------------------------------------:|
| Batch Synchronization  | MySQL          | [doc](docs/connectors/mysql/mysql-source.md)              | [doc](docs/connectors/mysql/mysql-sink.md)                |[doc](docs/connectors/mysql/mysql-lookup.md)               |
|                        | TiDB           |                                                           | 参考mysql                                                  |参考mysql                                                  |   
|                        | Oracle         | [doc](docs/connectors/oracle/oracle-source.md)            | [doc](docs/connectors/oracle/oracle-sink.md)              |[doc](docs/connectors/oracle/oracle-lookup.md)             |
|                        | SqlServer      | [doc](docs/connectors/sqlserver/sqlserver-source.md)      | [doc](docs/connectors/sqlserver/sqlserver-sink.md)        |[doc](docs/connectors/sqlserver/sqlserver-lookup.md)       |
|                        | PostgreSQL     | [doc](docs/connectors/postgres/postgres-source.md)        | [doc](docs/connectors/postgres/postgres-sink.md)          |[doc](docs/connectors/postgres/postgres-lookup.md)         |
|                        | DB2            | [doc](docs/connectors/db2/db2-source.md)                  | [doc](docs/connectors/db2/db2-sink.md)                    |[doc](docs/connectors/db2/db2-lookup.md)                   |
|                        | ClickHouse     | [doc](docs/connectors/clickhouse/clickhouse-source.md)    | [doc](docs/connectors/clickhouse/clickhouse-sink.md)      |[doc](docs/connectors/clickhouse/clickhouse-lookup.md)     |
|                        | Greenplum      | [doc](docs/connectors/greenplum/greenplum-source.md)      | [doc](docs/connectors/greenplum/greenplum-sink.md)        |                                                           |
|                        | KingBase       | [doc](docs/connectors/kingbase/kingbase-source.md)        | [doc](docs/connectors/kingbase/kingbase-sink.md)          |                                                           |
|                        | MongoDB        | [doc](docs/connectors/mongodb/mongodb-source.md)          | [doc](docs/connectors/mongodb/mongodb-sink.md)            |[doc](docs/connectors/mongodb/mongodb-lookup.md)           |
|                        | SAP HANA       | [doc](docs/connectors/saphana/saphana-source.md)          | [doc](docs/connectors/saphana/saphana-sink.md)            |                                                           |  
|                        | ElasticSearch7 | [doc](docs/connectors/elasticsearch7/es7-source.md)       | [doc](docs/connectors/elasticsearch7/es7-lookup.md)       |[doc](docs/connectors/elasticsearch7/es7-sink.md)          |
|                        | FTP            | [doc](docs/connectors/ftp/ftp-source.md)                  | [doc](docs/connectors/ftp/ftp-sink.md)                    |                                                           |
|                        | HDFS           | [doc](docs/connectors/hdfs/hdfs-source.md)                | [doc](docs/connectors/hdfs/hdfs-sink.md)                  |                                                           |
|                        | Stream         | [doc](docs/connectors/stream/stream-source.md)            | [doc](docs/connectors/stream/stream-sink.md)              |                                                           |
|                        | Redis          |                                                           | [doc](docs/connectors/redis/redis-sink.md)                |[doc](docs/connectors/redis/redis-lookup.md)               |
|                        | Hive           |                                                           | [doc](docs/connectors/hive/hive-sink.md)                  |                                                           |
|                        | Hbase          | [doc](docs/connectors/hbase/hbase-source.md)              | [doc](docs/connectors/hbase/hbase-sink.md)                |[doc](docs/connectors/hbase/hbase-lookup.md)               |
|                        | Solr           | [doc](docs/connectors/solr/solr-source.md)                | [doc](docs/connectors/solr/solr-sink.md)                  |                                                           |
|                        | File           |  [doc](docs/connectors/file/file-source.md)               |                                                           |                                                           |
| Stream Synchronization | Kafka          | [doc](docs/connectors/kafka/kafka-source.md)              | [doc](docs/connectors/kafka/kafka-sink.md)                |                                                           |
|                        | EMQX           | [doc](docs/connectors/emqx/emqx-source.md)                | [doc](docs/connectors/emqx/emqx-sink.md)                  |                                                           |
|                        | MySQL Binlog   | [doc](docs/connectors/binlog/binlog-source.md)            |                                                           |                                                           |
|                        | Oracle LogMiner | [doc](docs/connectors/logminer/LogMiner-source.md)       |                                                           |                                                           |
|                        | Sqlserver CDC | [doc](docs/connectors/sqlservercdc/SqlserverCDC-source.md) |                                                           |                                                           |      

# 快速开始

请点击[快速开始](docs/quickstart.md)

# 通用配置

请点击[插件通用配置](docs/generalconfig.md)

# 统计指标

请点击[统计指标](docs/statistics.md)

# Iceberg
请点击 [Iceberg](docs/iceberg.md)

# Kerberos

请点击[Kerberos](docs/kerberos.md)

# Questions

请点击[Questions](docs/questions.md)

# 如何贡献Chunjun

请点击[如何贡献Chunjun](docs/contribution.md)

# License

Chunjun is under the Apache 2.0 license. See
the [LICENSE](http://www.apache.org/licenses/LICENSE-2.0) file for details.
