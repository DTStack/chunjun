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

- The Reader plugin for relational databases supports interval polling. It can continuously collect changing data; [Interval Polling](docs/rdbreader.md)

- Some databases support opening Kerberos security authentication;  [Kerberos](docs/kerberos.md)

- Limit the reading speed of Reader plugins and reduce the impact on business databases;

- Save the dirty data when writing data;

- Limit the maximum number of dirty data;

- Multiple running modes: Local,Standalone,Yarn Session,Yarn Per;

The following databases are currently supported:

|                        | Database Type | Reader | Writer |
|:----------------------:|:-------------:|:------:|:------:|
| Batch Synchronization  | MySQL         | √      | √      |
|                        | Oracle        | √      | √      |
|                        | SqlServer     | √      | √      |
|                        | PostgreSQL    | √      | √      |
|                        | DB2           | √      | √      |
|                        | GBase         | √      | √      |
|                        | ClickHouse    | √      | √      |
|                        | PolarDB       | √      | √      |
|                        | SAP Hana      | √      | √      |
|                        | Teradata      | √      | √      |
|                        | Phoenix       | √      | √      |
|                        | Cassandra     | √      | √      |
|                        | ODPS          | √      | √      |
|                        | HBase         | √      | √      |
|                        | MongoDB       | √      | √      |
|                        | Kudu          | √      | √      |
|                        | ElasticSearch | √      | √      |
|                        | FTP           | √      | √      |
|                        | HDFS          | √      | √      |
|                        | Carbondata    | √      | √      |
|                        | Redis         | √      |        |
|                        | Hive          |        | √      |
| Stream Synchronization | Kafka         | √      | √      |
|                        | EMQX          | √      | √      |
|                        | MySQL Binlog  | √      |        |

# Documentation

[Documentation](https://github.com/DTStack/flinkx/wiki) | [Old Documentation](README_OLD.md)

# License

FlinkX is under the Apache 2.0 license. See the [LICENSE](http://www.apache.org/licenses/LICENSE-2.0) file for details.
