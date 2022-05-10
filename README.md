Chunjun
============

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

English | [中文](README_CH.md)

# Communication

- We are recruiting **Big data platform development engineers**.If you want more information about the position, please add WeChat ID [**ysqwhiletrue**] or email your resume to [sishu@dtstack.com](mailto:sishu@dtstack.com).

- We use [DingTalk](https://www.dingtalk.com/) to communicate,You can search the group number [**30537511**] or scan the QR code below to join the communication group

  <div align=center>
     <img src=docs/images/IMG_3362.JPG width=300 />
   </div>

# Introduction

*[Chunjun 1.12 New Features](docs/changeLog.md)*

Chunjun is a data synchronization tool based on Flink. Chunjun can collect static data, such as MySQL, HDFS, etc, as well as real-time changing data, such as MySQL binlog, Kafka, etc. **At the same time, Chunjun is also a computing framework that supports all the syntax and features of native FlinkSql** , <big>**And provide a large number of [cases](Chunjun-examples)**</big>. Chunjun currently includes the following features:

- Most plugins support concurrent reading and writing of data, which can greatly improve the speed of reading and writing;

- Some plug-ins support the function of failure recovery, which can restore tasks from the failed location and save running time; [Failure Recovery](docs/restore.md)

- The source plugin for relational databases supports interval polling. It can continuously collect changing data; [Interval Polling](docs/offline/reader/mysqlreader.md)

- Some databases support opening Kerberos security authentication;  [Kerberos](docs/kerberos.md)

- Limit the reading speed of source plugins and reduce the impact on business databases;

- Save the dirty data when writing data;

- Limit the maximum number of dirty data;

- Multiple running modes: Local,Standalone,Yarn Session,Yarn Per;

- **Synchronization tasks support transformer operations that execute flinksql syntax;**

- **sql task support is [shared](docs/conectorShare.md) with flinkSql's own connectors;**

The following databases are currently supported:

|                        | Database Type  | Source                          | Sink                          | Lookup
|:----------------------:|:--------------:|:-------------------------------:|:-------------------------------:|:-------------------------------:|
| Batch Synchronization  | MySQL          | [doc](docs/connectors/mysql/mysql-source.md)        | [doc](docs/connectors/mysql/mysql-sink.md)      |[doc](docs/connectors/mysql/mysql-lookup.md)      |
|                        | TiDB           || reference mysql                                 |reference mysql                                   |   
|                        | Oracle         | [doc](docs/connectors/oracle/oracle-source.md)       | [doc](docs/connectors/oracle/oracle-sink.md)     |[doc](docs/connectors/oracle/oracle-lookup.md)      |
|                        | Doris          |                                 | [doc](docs/connectors/doris/dorisbatch-sink.md)     |                    |
|                        | SqlServer      | [doc](docs/connectors/sqlserver/sqlserver-source.md)    | [doc](docs/connectors/sqlserver/sqlserver-sink.md)  |[doc](docs/connectors/sqlserver/sqlserver-lookup.md)
|                        | PostgreSQL     | [doc](docs/connectors/postgres/postgres-source.md) | [doc](docs/connectors/postgres/postgres-sink.md) | [doc](docs/connectors/postgres/postgres-lookup.md) |
|                        | DB2            | [doc](docs/connectors/db2/db2-source.md)          | [doc](docs/connectors/db2/db2-sink.md)        | [doc](docs/connectors/db2/db2-lookup.md)
|                        | ClickHouse     | [doc](docs/connectors/clickhouse/clickhouse-source.md)   | [doc](docs/connectors/clickhouse/clickhouse-sink.md) | [doc](docs/connectors/clickhouse/clickhouse-lookup.md)      |
|                        | Greenplum      | [doc](docs/connectors/greenplum/greenplum-source.md)    | [doc](docs/connectors/greenplum/greenplum-sink.md)  |
|                        | KingBase       | [doc](docs/connectors/kingbase/kingbase-source.md)     | [doc](docs/connectors/kingbase/kingbase-sink.md)   |
|                        | MongoDB        | [doc](docs/connectors/mongodb/mongodb-source.md) | [doc](docs/connectors/mongodb/mongodb-sink.md) |[doc](docs/connectors/mongodb/mongodb-lookup.md) |
|                        | SAP HANA  | [doc](docs/connectors/saphana/saphana-source.md)           | [doc](docs/connectors/saphana/saphana-sink.md)         |
|                        | ElasticSearch7 | [doc](docs/connectors/elasticsearch7/es7-source.md) | [doc](docs/connectors/elasticsearch7/es7-sink.md) | [doc](docs/connectors/elasticsearch7/es7-sink.md) |
|                        | FTP            | [doc](docs/connectors/ftp/ftp-source.md)          | [doc](docs/connectors/ftp/ftp-sink.md)        |
|                        | HDFS           | [doc](docs/connectors/hdfs/hdfs-source.md)         | [doc](docs/connectors/hdfs/hdfs-sink.md)       |
|                        | Stream         | [doc](docs/connectors/stream/stream-source.md)       | [doc](docs/connectors/stream/stream-sink.md) |
|                        | Redis          |                                                  | [doc](docs/connectors/redis/redis-sink.md)      |[doc](docs/connectors/redis/redis-lookup.md)      |
|                        | Hive           |                                                  | [doc](docs/connectors/hive/hive-sink.md)       |
|                        | Solr          | [doc](docs/connectors/solr/solr-source.md)        | [doc](docs/connectors/solr/solr-sink.md)       |
|                        | File           |  [doc](docs/connectors/file/file-source.md)
|                        | StarRocks      |                                                  | [doc](docs/connectors/starrocks/starrocks-sink.md) |
| Stream Synchronization | Kafka          | [doc](docs/connectors/kafka/kafka-source.md)       | [doc](docs/connectors/kafka/kafka-sink.md)     |
|                        | EMQX           | [doc](docs/connectors/emqx/emqx-source.md)        | [doc](docs/connectors/emqx/emqx-sink.md)      |
|                        | MySQL Binlog   | [doc](docs/connectors/binlog/binlog-source.md)      |                                                |
|                        | Oracle LogMiner | [doc](docs/connectors/logminer/LogMiner-source.md)   |                                            |
|                        | Sqlserver CDC | [doc](docs/connectors/sqlservercdc/SqlserverCDC-source.md) |                                                |
|                        | Postgres  CDC | [doc](docs/connectors/pgwal/Postgres-CDC.md) |                                                |

# Quick Start

Please click [Quick Start](docs/quickstart.md)

# General Configuration

Please click [General Configuration](docs/generalconfig.md)

# Statistics Metric

Please click [Statistics Metric](docs/statistics.md)

# Iceberg

Please click [Iceberg](docs/iceberg.md)

# Kerberos

Please click [Kerberos](docs/kerberos.md)

# Questions

Please click [Questions](docs/questions.md)

# How to contribute Chunjun

Please click [Contribution](docs/contribution.md)

# License

Chunjun is under the Apache 2.0 license. See the [LICENSE](http://www.apache.org/licenses/LICENSE-2.0) file for details.
