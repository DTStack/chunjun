# Postgres CDC Source


<!-- TOC -->

- [Ⅰ、Introduction](#Ⅰ、Introduction)
- [Ⅱ、Supported Versions](#Ⅱ、Supported Versions)
- [Ⅲ、Plugin name](#Ⅲ、Plugin name)
- [Ⅳ、Database Configuration](#Ⅳ、Database Configuration)
- [Ⅴ、Parameter description](#Ⅴ、Parameter description)
    - [1 、Sync](#1sync)
    - [2 、SQL](#2sql)
- [Ⅵ、Data Type](#Ⅵ、Data Type)
- [Ⅶ、Script example](#Ⅶ、Script example)

<!-- /TOC -->

## Ⅰ、Introduction
The Postgres CDC plugin captures change data from Postgres in real time. Currently, the sink plugin does not support data restoration, and can only write changed log data.

## Ⅱ、Supported version
Postgres 10.0+

## Ⅲ、Plugin name
| Sync | pgwalsource、pgwalreader |
| --- | --- |
| SQL | pgwal-x |

##  Ⅳ、Database Configuration
1. The write-ahead log level (wal_level) must be logical
2. The plugin is implemented based on the PostgreSQL logical replication and logical decoding functions, so the PostgreSQL account has at least replication permissions, and if it is allowed to create slots, it has at least super administrator permissions
3. For detailed principles, please refer to the official PostgreSQL documentation http://postgres.cn/docs/10/index.html



##  Ⅴ、Parameter description
###  1、Sync

- **url**
    - Description：JDBC URL link for Postgresql
    - Required：yes
    - Parameter type: string
    - Default value: none
      
    <br />

- **username**
  - Description: username
  - Required: yes
  - Parameter type: string
  - Default value: none
    
    <br />

- **password**
  - Description: Password
  - Required: yes
  - Parameter type: string
  - Default value: none

    <br />

- **databaseName**
    - Description：the database name
    - Required：yes
    - Parameter type：string
    - Default value：none
      
    <br />

- **tableList**
    - Description：List of tables to be parsed
    - Notice：After specifying this parameter, the filter parameter will be invalid, the table and filter are empty, listen to all tables under the schema in jdbcUrl
    - Required：no
    - Parameter type：list<string>
    - Default value：none
      
    <br />

- **slotName**
    - Description：slot name
    - Required：no
    - Parameter type：String
    - Default value：true
      
    <br />

- **allowCreated**
    - Description：Whether to automatically create a slot
    - Required：no
    - Parameter type：boolean
    - Default value：false
      
    <br />

- **temporary**
    - Description：Whether it is a temporary slot
    - Required：no
    - Parameter type：boolean
    - Default value：false
      
    <br />

- **statusInterval**
    - Description：Heartbeat interval
    - Required：no
    - Parameter type：int
    - Default value：10
      
    <br />

- **lsn**
    - Description：Log sequence number
    - Required：no
    - Parameter type：long
    - Default value：0
      
    <br />

- **slotAvailable**
    - Description：Is the slot available
    - Required：no
    - Parameter type：boolean
    - Default value：false
      
    <br />


## Ⅵ、Data Type
| Support | BIT |
| --- | --- |
|  | NULL、 BOOLEAN、 TINYINT、 SMALLINT、 INTEGER、 INTERVAL_YEAR_MONTH、 BIGINT|
|  | INTERVAL_DAY_TIME、 DATE、 TIME_WITHOUT_TIME_ZONE |
|  | TIMESTAMP_WITHOUT_TIME_ZONE、 TIMESTAMP_WITH_LOCAL_TIME_ZONE、 FLOAT |
|  | DOUBLE、 CHAR、 VARCHAR、 DECIMAL、 BINARY、 VARBINARY |
| Not supported yet | none |


## Ⅶ、Script example
See the `chunjun-examples` folder in the project.

