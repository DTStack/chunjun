# Mysql Sink

## 1. Introduction

mysql sink

## 2. Support Version

mysql5.x

## 3. Plugin Name

| Sync | mysqlsink、mysqlwriter |
| --- | --- |
| SQL | mysql-x |

## 4. Parameter

### (1) Sync

- **connection**
    - definition：Database connection parameters, including JDBC URL, schema, table and other parameters
    - necessary：true
    - data type：List
    - default：null
      ```text
      "connection": [{
       "jdbcUrl": ["jdbc:mysql://0.0.0.1:3306/database?useSSL=false"],
       "table": ["table"],
       "schema":"public"
      }]
      ```
       <br />

- **jdbcUrl**
    - definition：JDBC connection string for relational database,search document for detail information of jdbcUrl：[MySQL官方文档](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)
    - necessary：true
    - data type：string
    - default：null
      <br />

- **schema**
    - definition：database schema name
    - necessary：false
    - data type：string
    - default：null
      <br />

- **table**
    - definition：the table name of the destination table. Currently only supports the configuration of a single table, and will support multiple tables in the future.
    - necessary：true
    - data type：List
    - default：null
      <br />

- **username**
    - definition：the user name of the data source
    - necessary：true
    - data type：String
    - default：null
      <br />

- **password**
    - definition：The password for the specified user name of the data source
    - necessary：true
    - data type：String
    - default：null
      <br />

- **column**
    - definition：The fields in the destination table that need to write data are separated by commas.eg."column": ["id","name","age"]
    - necessary：true
    - data type：List
    - default：null
      <br />

- **fullColumn**
    - definition：All fields in the destination table are separated by commas.eg. "column": ["id","name","age","hobby"]，if not configured, it will be obtained in the system table
    - necessary：false
    - data type：List
    - default：null
      <br />

- **preSql**
    - definition：Before writing data to the destination table, a set of standard statements here will be executed first.
    - necessary：false
    - data type：List
    - default：null
      <br />

- **postSql**
    - definition：After writing data to the destination table, a set of standard statements here will be executed.
    - necessary：false
    - data type：List
    - default：null
      <br />

- **writeMode**
    - definition：Use insert into or replace into or ON DUPLICATE KEY UPDATE statement to control writing data to the target table.
    - necessary：true
    - options：insert/replace/update
    - data type：String
    - default：insert
      <br />

- **batchSize**
    - definition：The size of the number of records submitted in batches at one time. This value can greatly reduce the number of network interactions between FlinkX and the database and improve the overall throughput. However, setting this value too large may cause the OOM situation of FlinkX running process.
    - necessary：false
    - data type：int
    - default：1024
      <br />

- **updateKey**
    - definition：When the write mode is update and replace, you need to specify the value of this parameter as a unique index field
    - Attention：
        - If this parameter is empty and the write mode is update and replace, the application will automatically obtain the unique index in the database;
        - If the data table does not have a unique index, but the write mode is configured as update and replace, the application will write the data in insert;
    - necessary：false
    - data type：Map<String,List>
        - eg."updateKey": {"key": ["id"]}
    - default：null
      <br />

- **semantic**
    - definition：Whether to enable two-phase commit on the sink side
    - Attention：
        - If this parameter is empty, two-phase commit is not enabled by default, that is, the sink side does not support exactly_once semantics;
        - Currently only supports exactly-once and at-least-once
    - necessary：false
    - data type：String
        - eg."semantic": "exactly-once"
    - default：at-least-once
      <br />

### (2) SQL

- **connector**
    - definition：mysql-x
    - necessary：true
    - data type：String
    - default：null
      <br />

- **url**
    - definition：jdbc:mysql://localhost:3306/test
    - necessary：true
    - data type：String
    - default：null
      <br />

- **table-name**
    - definition：the name of table
    - necessary：true
    - data type：String
    - default：null：
      <br />

- **username**
    - definition：username
    - necessary：true
    - data type：String
    - default：null
      <br />

- **password**
    - definition：password
    - necessary：true
    - data type：String
    - default：null
      <br />

- **sink.buffer-flush.max-rows**
    - definition：Number of batch write data，Unit: Piece
    - necessary：false
    - data type：String
    - default：1024
      <br />

- **sink.buffer-flush.interval**
    - definition：Batch write interval，Unit: milliseconds
    - necessary：false
    - data type：String
    - default：10000
      <br />

- **sink.all-replace**
    - definition：Whether to replace all the data in the database (if the original value in the database is not null, the new value is null, if it is true, it will be replaced with null)
    - necessary：false
    - data type：String
    - default：false
      <br />

- **sink.parallelism**
    - definition：Parallelism of writing results
    - necessary：false
    - data type：String
    - default：null
      <br />

- **sink.semantic**
    - definition：Whether to enable two-phase commit on the sink side
    - Attention：
        - If this parameter is empty, two-phase commit is not enabled by default, that is, the sink side does not support exactly_once semantics；
        - Currently only supports exactly-once and at-least-once
    - necessary：false
    - data type：String
        - eg."semantic": "exactly-once"
    - default：at-least-once
      <br />

## 5. Data Type

| support | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| unSupport | ARRAY、MAP、STRUCT、UNION |

## 6. Profile Demo

see`flinkx-examples`directory.
