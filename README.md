FlinkX
============

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

English | [中文](README_CH.md)

# Communication

- We are recruiting **Big data platform development engineers**.If you want more information about the position, please add WeChat ID [**ysqwhiletrue**] or email your resume to [sishu@dtstack.com](mailto:sishu@dtstack.com).

- We use [DingTalk](https://www.dingtalk.com/) to communicate,You can search the group number [**30537511**] or scan the QR code below to join the communication group
  
  <div align=center>
     <img src=docs/images/ding.jpg width=300 />
   </div>

# Introduction

FlinkX is a data synchronization tool based on Flink. FlinkX can collect static data, such as MySQL, HDFS, etc, as well as real-time changing data, such as MySQL binlog, Kafka, etc. FlinkX currently includes the following features:

- Most plugins support concurrent reading and writing of data, which can greatly improve the speed of reading and writing;

- Some plug-ins support the function of failure recovery, which can restore tasks from the failed location and save running time; [Failure Recovery](docs/restore.md)

- The Reader plugin for relational databases supports interval polling. It can continuously collect changing data; [Interval Polling](docs/offline/reader/mysqlreader.md)

- Some databases support opening Kerberos security authentication;  [Kerberos](docs/kerberos.md)

- Limit the reading speed of Reader plugins and reduce the impact on business databases;

- Save the dirty data when writing data;

- Limit the maximum number of dirty data;

- Multiple running modes: Local,Standalone,Yarn Session,Yarn Per;

The following databases are currently supported:

|                        | Database Type  | Reader                          | Writer                          |
|:----------------------:|:--------------:|:-------------------------------:|:-------------------------------:|
| Batch Synchronization  | MySQL          | [doc](docs/offline/reader/mysqlreader.md)        | [doc](docs/offline/writer/mysqlwriter.md)      |
|                        | Oracle         | [doc](docs/offline/reader/oraclereader.md)       | [doc](docs/offline/writer/oraclewriter.md)     |
|                        | SqlServer      | [doc](docs/offline/reader/sqlserverreader.md)    | [doc](docs/offline/writer/sqlserverwriter.md)  |
|                        | PostgreSQL     | [doc](docs/offline/reader/postgresqlreader.md)   | [doc](docs/offline/writer/postgresqlwriter.md) |
|                        | DB2            | [doc](docs/offline/reader/db2reader.md)          | [doc](docs/offline/writer/db2writer.md)        |
|                        | GBase          | [doc](docs/offline/reader/gbasereader.md)        | [doc](docs/offline/writer/gbasewriter.md)      |
|                        | ClickHouse     | [doc](docs/offline/reader/clickhousereader.md)   | [doc](docs/offline/writer/clickhousewriter.md) |
|                        | PolarDB        | [doc](docs/offline/reader/polardbreader.md)      | [doc](docs/offline/writer/polardbwriter.md)    |
|                        | SAP Hana       | [doc](docs/offline/reader/saphanareader.md)      | [doc](docs/offline/writer/saphanawriter.md)    |
|                        | Teradata       | [doc](docs/offline/reader/teradatareader.md)     | [doc](docs/offline/writer/teradatawriter.md)   |
|                        | Phoenix        | [doc](docs/offline/reader/phoenixreader.md)      | [doc](docs/offline/writer/phoenixwriter.md)    |
|                        | 达梦            | [doc](docs/offline/reader/dmreader.md)           | [doc](docs/offline/writer/dmwriter.md)        |
|                        | Greenplum      | [doc](docs/offline/reader/greenplumreader.md)    | [doc](docs/offline/writer/greenplumwriter.md)  |
|                        | KingBase       | [doc](docs/offline/reader/kingbasereader.md)     | [doc](docs/offline/writer/kingbasewriter.md)   |
|                        | Cassandra      | [doc](docs/offline/reader/cassandrareader.md)    | [doc](docs/offline/writer/cassandrawriter.md)  |
|                        | ODPS           | [doc](docs/offline/reader/odpsreader.md)         | [doc](docs/offline/writer/odpswriter.md)       |
|                        | HBase          | [doc](docs/offline/reader/hbasereader.md)        | [doc](docs/offline/writer/hbasewriter.md)      |
|                        | MongoDB        | [doc](docs/offline/reader/mongodbreader.md)      | [doc](docs/offline/writer/mongodbwriter.md)    |
|                        | Kudu           | [doc](docs/offline/reader/kudureader.md)         | [doc](docs/offline/writer/kuduwriter.md)       |
|                        | ElasticSearch  | [doc](docs/offline/reader/esreader.md)           | [doc](docs/offline/writer/eswriter.md)         |
|                        | FTP            | [doc](docs/offline/reader/ftpreader.md)          | [doc](docs/offline/writer/ftpwriter.md)        |
|                        | HDFS           | [doc](docs/offline/reader/hdfsreader.md)         | [doc](docs/offline/writer/hdfswriter.md)       |
|                        | Carbondata     | [doc](docs/offline/reader/carbondatareader.md)   | [doc](docs/offline/writer/carbondatawriter.md) |
|                        | Stream         | [doc](docs/offline/reader/streamreader.md)       | [doc](docs/offline/writer/carbondatawriter.md) |
|                        | Redis          |                                                  | [doc](docs/offline/writer/rediswriter.md)      |
|                        | Hive           |                                                  | [doc](docs/offline/writer/hivewriter.md)       |
| Stream Synchronization | Kafka          | [doc](docs/realTime/reader/kafkareader.md)       | [doc](docs/realTime/writer/kafkawriter.md)     |
|                        | EMQX           | [doc](docs/realTime/reader/emqxreader.md)        | [doc](docs/realTime/writer/emqxwriter.md)      |
|                        | RestApi        || [doc](docs/realTime/writer/restapiwriter.md)   |
|                        | MySQL Binlog   | [doc](docs/realTime/reader/binlogreader.md)      |                                                |
|                        | MongoDB Oplog  | [doc](docs/realTime/reader/mongodboplogreader.md)|                                                |
|                        | PostgreSQL WAL | [doc](docs/realTime/reader/pgwalreader.md)       |                                                |

# Quick Start

Please click [Quick Start](docs/quickstart.md)

# General Configuration

Please click [General Configuration](docs/generalconfig.md)

# Statistics Metric

Please click [Statistics Metric](docs/statistics.md)

# Kerberos

Please click [Kerberos](docs/kerberos.md)

# Questions

Please click [Questions](docs/questions.md)

# How to contribute FlinkX

Please click [Contribution](docs/contribution.md)

# License

FlinkX is under the Apache 2.0 license. See the [LICENSE](http://www.apache.org/licenses/LICENSE-2.0) file for details.
