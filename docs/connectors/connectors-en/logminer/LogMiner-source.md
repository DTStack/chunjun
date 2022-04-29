# LogMiner Source

<!-- TOC -->

- [LogMiner Source](#logminer-source)
    - [I、Introduction](#I Introduction)
    - [II、Supported Versions](#II Supported Versions)
    - [III、Database Configuration](#III Database Configuration)
    - [IV、LogMinerPrinciple](#IV logminer principle)
    - [V、Plugin Name](#V Plugin Name)
    - [VI、Parameter Description](#VI Parameter Description)
        - [i、Sync](#isync)
        - [ii、SQL](#iisql)
    - [VII、Data Structure](#VII Data Structure)
    - [VIII、Data Types](#VIII Data Types)
    - [Ⅸ、Script Example](#ⅨScript Example)

<!-- /TOC -->

<br/>

## I、Introduction

The oraclelogminer plug-in supports configuring the name of the listening table and the starting point for reading log data. Oraclelogminer saves the current consumption points during checkpoint, so it supports continuation.<br/>

## II、Supported Versions

oracle10,oracle11,oracle12,oracle19,RAC,master-slave
<br/>

## III、Database Configuration

[OracleConfigurationLogMiner](LogMinerConfiguration.md)
<br/>

## IV、LogMinerPrinciple

[LogMinerPrinciple](LogMinerPrinciple.md)
<br/>

## V、Plugin Name

| Sync | oraclelogminerreader、oraclelogminersource |
| --- | --- |
| SQL | oraclelogminer-x |

##  

## VI、Parameter Description

### i、Sync

- **jdbcUrl**
    - Description：jdbc url of Oracle database,
    - Required：Yes
    - Field type：string
    - Default value：none

<br/>

- **username**
    - Description：The username of the data source
    - Required：Yes
    - Field type：string
    - Default：none

<br/>

- **password**
    - Description：The password of the username specified by the data source
    - Required：Yes
    - Field type：string
    - Default：none

<br/>

- **table**
    - Description： The format of the table to be monitored is: schema.table. Schema cannot be configured as *, but table can be configured to * monitor all tables under the specified library, such as: [schema1. Table1 "," schema1. Table2 "," schema2. *]
    - Required：No
    - Field type：array
    - Default：none

<br/>

- **splitUpdate**
    - Description：When the data update type is update, whether to split the update into two data, see [七、Data Structure](#七Data Structure)
    - Required: No
    - Field type: boolean
    - Default value: false

<br/>

- **cat**
    - Description：Type of operation data monitored,including insert, update, and delete
    - Required：No
    - Field type：String
    - Default value：UPDATE,INSERT,DELETE

<br/>

- **readPosition**
    - Description：Starting point of Oracle Logminer
    - Optional value：
        - all： Start collection from the oldest archive log group in the Oracle database (not recommended)
        - current：Start collection from task runtime
        - time： Start collection from a specified point in time
        - scn： Start acquisition from the specified SCN number
    - Required：No
    - Field type：String
    - Default value：current

<br/>

- **startTime**
    - Description： Specifies the millisecond timestamp of the collection start point
    - Required：This parameter is required when 'readposition' is' time '
    - Field type：Long(Millisecond timestamp)
    - Default value：none

<br/>

- **startSCN**
    - Description： Specifies the SCN number of the collection start point
    - Required：This parameter is required when 'readposition' is' SCN '
    - Field type：String
    - Default value：none

<br/>

- **fetchSize**
    - Description： Batch from v$logmnr_contents The number of data pieces pulled in the contents view. For data changes with a large amount of data, increasing this value can increase the reading speed of the task to a certain extent
    - Required：No
    - Field type：Integer
    - Default value：1000

<br/>

- **queryTimeout**
    - Description： Timeout parameter of logminer executing SQL query, unit: seconds
    - Required：No
    - Field typ：Long
    - Default value：300

<br/>

- **supportAutoAddLog**
    - Description：Whether to automatically add log groups when starting logminer (not recommended)
    - Required：No
    - Field typ：Boolean
    - Default value：false

<br/>

- **pavingData**
    - Description：Whether to flatten the parsed JSON data, see [七、Data Structure](#七Data Structure)
    - Required：No
    - Field typ：boolean
    - Default value：false

<br/>

### ii、SQL

- **url**
    - Description：jdbc url of Oracle database,
    - Required：Yes
    - Field type：string
    - Default value：none

<br/>

- **username**
    - Description：The username of the data source
    - Required：Yes
    - Field type：string
    - Default：none

<br/>

- **password**
    - Description：The password of the username specified by the data source
    - Required：Yes
    - Field type：string
    - Default：none

<br/>

- **table**
    - Description： The format of the table to be monitored is: schema.table.
    - Note：SQL tasks only support listening to a single table
    - Required：No
    - Field type：string
    - Default：none

<br/>

- **cat**
    - Description：Type of operation data monitored,including insert, update, and delete
    - Required：No
    - Field type：String
    - Default value：UPDATE,INSERT,DELETE

<br/>

- **read-position**
    - Description：Starting point of Oracle Logminer
    - Optional value：
        - all： Start collection from the oldest archive log group in the Oracle database (not recommended)
        - current：Start collection from task runtime
        - time： Start collection from a specified point in time
        - scn： Start acquisition from the specified SCN number
    - Required：No
    - Field type：String
    - Default value：current

<br/>

- **start-time**
    - Description： Specifies the millisecond timestamp of the collection start point
    - Required：This parameter is required when 'read-position' is' time '
    - Field type：Long(Millisecond timestamp)
    - Default value：none

<br/>

- **start-scn**
    - Description： Specifies the SCN number of the collection start point
    - Required：This parameter is required when 'read-position' is' SCN '
    - Field type：String
    - Default value：none

<br/>

- **fetch-size**
    - Description： Batch from v$logmnr_contents The number of data pieces pulled in the contents view. For data changes with a large amount of data, increasing this value can increase the reading speed of the task to a certain extent
    - Required：No
    - Field type：Integer
    - Default value：1000

<br/>

- **query-timeout**
    - Description： Timeout parameter of logminer executing SQL query, unit: seconds
    - Required：No
    - Field type：Long
    - Default value：300

<br/>

- **support-auto-add-log**
    - Description：Whether to automatically add log groups when starting logminer (not recommended)
    - Required：No
    - Field type：Boolean
    - Default value：false

<br/>

- **io-threads**
    - Description：The maximum number of IO processing threads is three
    - Required：No
    - Field type：int
    - Default value：1

<br/>

- **max-log-file-size**
    - Description：the size of the log file loaded at one time. The default is 5g. The unit is byte
    - Required：No
    - Field type：long
    - Default value：5*1024*1024*1024

<br/>

- **transaction-cache-num-size**
    - Description：The number of DML SQL that logminer cache
    - Required：No
    - Field type：long
    - Default value：800

<br/>

- **transaction-expire-time**
    - Description：The expiration time of logMiner cache, unit minutes
    - Required：No
    - Field type：int
    - Default value：20

<br/>
## VII、Data Structure

execute sql at 2021-06-29 23:42:19(timeStamp：1624981339000)：

```sql
INSERT INTO TIEZHU.RESULT1 ("id", "name", "age") VALUES (1, 'a', 12)
```

<br/>

execute sql at 2021-06-29 23:42:29(timeStamp：1624981349000)：

```sql
UPDATE TIEZHU.RESULT1 t SET t."id" = 2, t."age" = 112 WHERE t."id" = 1
```

<br/>

execute sql at 2021-06-29 23:42:34(timeStamp：1624981354000)：

```sql
 DELETE FROM TIEZHU.RESULT1 WHERE "id" = 2 
```

<br/>

1、pavingData = true, splitUpdate = false The data in rowdata is：

```
//scn schema, table, ts, opTime, type, before_id, before_name, before_age, after_id, after_name, after_age
[49982945,"TIEZHU", "RESULT1", 6815665753853923328, "2021-06-29 23:42:19.0", "INSERT", null, null, null, 1, "a", 12]
[49982969,"TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE", 1, "a", 12 , 2, "a", 112]
[49982973,"TIEZHU", "RESULT1", 6815665796140896256, "2021-06-29 23:42:34.0", "DELETE", 2, "a",112 , null, null, null]
```

<br/>

2、pavingData = false, splitUpdate = false The data in rowdata is：

```
//scn, schema, table,  ts, opTime, type, before, after
[49982945, "TIEZHU", "RESULT1", 6815665753853923328, "2021-06-29 23:42:19.0", "INSERT", null, {"id":1, "name":"a", "age":12}]
[49982969, "TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE", {"id":1, "name":"a", "age":12}, {"id":2, "name":"a", "age":112}]
[49982973, "TIEZHU", "RESULT1", 6815665796140896256, "2021-06-29 23:42:34.0", "DELETE", {"id":2, "name":"a", "age":112}, null]
```

<br/>

3、pavingData = true, splitUpdate = true The data in rowdata is：

```
//scn, schema, table, ts, opTime, type, before_id, before_name, before_age, after_id, after_name, after_age
[49982945,"TIEZHU", "RESULT1", 6815665753853923328, "2021-06-29 23:42:19.0", "INSERT", null, null, null, 1, "a",12 ]

//scn, schema, table, opTime, ts, type, before_id, before_name, before_age
[49982969, "TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE_BEFORE", 1, "a", 12]
//scn, schema, table, opTime, ts, type, after_id, after_name, after_age
[49982969, "TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE_AFTER", 2, "a", 112]

//scn, schema, table, ts, opTime, type, before_id, before_name, before_age, after_id, after_name, after_age
[49982973, "TIEZHU", "RESULT1", 6815665796140896256,  "2021-06-29 23:42:34.0", "DELETE", 2, "a", 112, null, null, null]


```

<br/>

4、pavingData = false, splitUpdate = true The data in rowdata is：

```
//scn, schema, table,  ts, opTime, type, before, after
[49982945, "TIEZHU", "RESULT1", 6815665753853923328, "2021-06-29 23:42:19.0", "INSERT", null, {"id":1, "name":"a", "age":12}]
//scn, schema, table,  ts, opTime, type, before
[49982969, "TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE_BEFORE", {"id":1, "name":"a", "age":12}]
//scn, schema, table,  ts, opTime, type, after
[49982969, "TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE_AFTER", {"id":2, "name":"a", "age":112}]
//scn, schema, table, ts, opTime, type, before, after
[49982973, "TIEZHU", "RESULT1", 6815665796140896256, "2021-06-29 23:42:34.0",  "DELETE", {"id":2, "name":"a", "age":112}, null]

```

<br/>

- scn：SCN number corresponding to Oracle database change record
- type：DML Type，INSERT，UPDATE、DELETE
- opTime：Execution time of SQL in database
- ts：Self incrementing ID, non duplicate, can be used for sorting. After decoding, it is the event time of flinkx. The decoding rules are as follows:
  <br/>

```java
long id = Long.parseLong("6815665753853923328");
        long res = id >> 22;
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sdf.format(res));		//2021-06-29 23:42:24
```

<br/>

## VIII、Data Types

| Support | NUMBER、SMALLINT、INT INTEGER、FLOAT、DECIMAL、NUMERIC、BINARY_FLOAT、BINARY_DOUBLE | | -- | -- | | | CHAR、NCHAR、NVARCHAR2、ROWID、VARCHAR2、VARCHAR、LONG、RAW、LONG RAW、INTERVAL YEAR、INTERVAL DAY、BLOB、CLOB、NCLOB | | | DATE、TIMESTAMP、TIMESTAMP WITH LOCAL TIME ZONE、TIMESTAMP WITH TIME ZONE | | Not Support | BFILE、XMLTYPE、Collections |


<br/>

## Ⅸ、Script Example

See the `flinkx-examples` folder in the project。
