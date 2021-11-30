# SqlserverCDC Source

<!-- TOC -->

- [I. Introduction](# I Introduction)
- [II. Supported Versions](# II Supported Versions)
- [III. Database Configuration](# III Database Configuration)
- [IV, SqlserverCDC principle] (# IV SqlserverCDC principle)
- [V, plug-in name] (# V plug-in name)
- [VI, parameter description] (# VI parameter description)
  - [1, Sync](#1sync)
  - [2, SQL](#2sql)
- vii. data structure](#seven data structure)
- [viii, data types](#viii data types)
- [ix, script example](#nine script example)

<!-- /TOC -->

<br/>

## I. Introduction

The Sqlservercdc plugin supports configuring the listener table name as well as reading the starting point to read log data. SQLservercdc saves the currently consumed lsn at checkpoint, so the support runs continuously.
<br/>

## II. Supported Versions

SqlServer 2012, 2014, 2016, 2017, 2019 standalone
<br/>

## III. Database configuration

[SqlserverCDC configuration](SqlserverCDC configuration.md)
<br/>

## IV. SqlserverCDC Principle

[SqlserverCDC Principle](SqlserverCDC Principle.md)
<br/>

## V. Plugin Name

| Sync | sqlservercdcreader, sqlservercdcsource |
| ---- | -------------------------------------- |
| SQL  | sqlservercdc-x                         |

<br/>

##

## VI. Parameter Description

### 1. Sync

- **url**
  - Description: JDBC URL link for sqlserver database
  - Required: yes
  - Parameter type: string
  - Default value: none

<br/>


- **username**
  - Description: username
  - Required: yes
  - Parameter type: string
  - Default value: none

<br/>

- **password**
  - Description: Password
  - Required: yes
  - Parameter type: string
  - Default value: none

<br/>

- **tableList**
  - Description: The tables to listen to, e.g. ["schema1.table1", "schema1.table2"].
  - Required: Yes
  - Field type: array
  - Default value: none

<br/>


- **splitUpdate**
  - Description: When the data update type is update, whether to split the update into two pieces of data, see [VI. Data Structure Description].
  - Required: No
  - Field type: boolean
  - Default value: false

<br/>

- **cat**
  - Description: The type of operation to be listened to, UPDATE, INSERT, DELETE are available, case insensitive, multiple, split by
  - Required: No
  - Field type: String
  - Default value: UPDATE,INSERT,DELETE

<br/>

- **lsn**
  - Description: The start position of the SqlServer CDC log sequence number to read
  - Required: No
  - Field Type: String(00000032:0000002038:0005)
  - Default value: None

<br/>

- **pollInterval**
  - Description: Listen to the interval of pulling SqlServer CDC database, the smaller the value, the smaller the collection delay time, the more pressure on the database access
  - Required: No
  - Field type: long (in milliseconds)
  - Default value: 1000

<br/>


- **pavingData**
  - Description: Whether to paving the parsed json data, see [VII. Description of data structure].
  - Required: No
  - Field type: boolean
  - Default value: false

<br/>

### 2、SQL

- **url**
  - Description: JDBC URL link for sqlserver database
  - Required: yes
  - Parameter type: string
  - Default value: none

<br/>

- **username**
  - Description: username
  - Required: yes
  - Parameter type: string
  - Default value: none

<br/>

- **password**
  - Description: Password
  - Required: yes
  - Parameter type: string
  - Default value: none

<br/>

- **table**
  - Description: The data table to be parsed.
  - Note: SQL task only supports listening to a single table and the data format is schema.table
  - Required: No
  - Field type: string
  - Default value: none

<br/>

- **cat**
  - Description: The type of operation to be listened to, UPDATE, INSERT, DELETE, case insensitive, multiple, split by
  - Required: No
  - Field type: String
  - Default value: UPDATE,INSERT,DELETE

<br/>

- **lsn**
  - Description: The start position of the SqlServer CDC log sequence number to read
  - Required: No
  - Field Type: String(00000032:0000002038:0005)
  - Default value: None

<br/>

- **poll-interval**
  - Description: Listen to the interval of pulling SqlServer CDC database, the smaller the value, the smaller the collection delay time, the more pressure on the database access
  - Required: No
  - Field type: long (in milliseconds)
  - Default value: 1000

<br/>

## VII. Data Structure

On 2020-01-01 12:30:00 (timestamp: 1577853000000) execute.

```sql
INSERT INTO `tudou`. `kudu`(`id`, `user_id`, `name`) VALUES (1, 1, 'a');
```

On 2020-01-01 12:31:00 (timestamp: 1577853060000) execute.

```sql
DELETE FROM `tudou`. `kudu` WHERE `id` = 1 AND `user_id` = 1 AND `name` = 'a';
```

On 2020-01-01 12:32:00 (timestamp: 1577853180000) execute.

```sql
UPDATE `tudou`. `kudu` SET `id` = 2, `user_id` = 2, `name` = 'b' WHERE `id` = 1 AND `user_id` = 1 AND `name` = 'a';
```

1. pavingData = true, splitUpdate = false
The data in RowData are, in order

```
//schema, table, ts, opTime, type, before_id, before_user_id, before_name, after_id, after_user_id, after_name
["tudou", "kudu", 6760525407742726144, 1577853000000, "INSERT", null, null, null, null, 1, 1, "a"]
["tudou", "kudu", 6760525407742726144, 1577853060000, "DELETE", 1, 1, "a", null, null, null, null]
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE", 1, 1, "a", 2, 2, "b" ]
```

2. pavingData = false, splitUpdate = false
The data in RowData are, in order

```
//schema, table, ts, opTime, type, before, after
["tudou", "kudu", 6760525407742726144, 1577853000000, "INSERT", null, {"id":1, "user_id":1, "name": "a"}]
["tudou", "kudu", 6760525407742726144, 1577853060000, "DELETE", {"id":1, "user_id":1, "name": "a"}, null]
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE", {"id":1, "user_id":1, "name": "a"}, {"id":2, "user_id":2, "name": "b"}]
```

3. pavingData = true, splitUpdate = true
The data in RowData are, in order

```
//schema, table, ts, opTime, type, before_id, before_user_id, before_name, after_id, after_user_id, after_name
["tudou", "kudu", 6760525407742726144, 1577853000000, "INSERT", null, null, null, null, 1, 1, "a"]
["tudou", "kudu", 6760525407742726144, 1577853060000, "DELETE", 1, 1, "a", null, null, null, null]

//schema, table, ts, opTime, type, before_id, before_user_id, before_name
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE_BEFORE", 1, 1, "a"]

//schema, table, ts, opTime, type, after_id, after_user_id, after_name
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE_AFTER", 2, 2, "b"]
```

4. pavingData = false, splitUpdate = true
The data in RowData are, in order

```
//schema, table, ts, opTime, type, before, after
["tudou", "kudu", 6760525407742726144, 1577853000000, "INSERT", null, {"id":1, "user_id":1, "name": "a"}]
["tudou", "kudu", 6760525407742726144, 1577853060000, "DELETE", {"id":1, "user_id":1, "name": "a"}, null]
//schema, table, ts, opTime, type, before
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE_BEFORE", {"id":1, "user_id":1, "name": "a"}]
//schema, table, ts, opTime, type, after
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE_AFTER", {"id":2, "user_id":2, "name": "b"}]
```

- type: change type, INSERT, UPDATE, DELETE
- opTime: the execution time of SQL in the database
- ts: self-incrementing ID, not repeated, can be used for sorting, decoded as the event time of FlinkX, decoding rules are as follows:

```java
long id = Long.parseLong("6760525407742726144");
long res = id >> 22;
DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
System.out.println(sdf.format(res)); //2021-01-28 19:54:21
```

## Eight, data types

| support       | BIT                                                          |
| ------------- | ------------------------------------------------------------ |
|               | TINYINT24, INT, INTEGER, FLOAT, DOUBLE, REAL, LONG, BIGINT, DECIMAL, NUMERIC |
|               | CHAR, VARCHAR, NCHAR, NVARCHAR, TEXT                         |
|               | DATE, TIME, TIMESTAMP, DATETIME, DATETIME2, SMALLDATETIME    |
|               | BINARY, VARBINARY                                            |
| Not supported | ROWVERSION, UNIQUEIDENTIFIER, CURSOR, TABLE, SQL_VARIANT     |


<br/>


## IX. Sample Scripts

See the `flinkx-examples` folder in the project.
