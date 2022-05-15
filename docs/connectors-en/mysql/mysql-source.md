# Mysql Source

## 1. Introduction
Supports offline reading from mysql and real-time interval polling reading from mysql.

## 2. Support Version
mysql5.x


## 3. Plugin Name
| Sync | mysqlsource、mysqlreader |
| --- | --- |
| SQL | mysql-x |


## 4. Parameter
### (1) Sync
- **connection**
  - definition：Database connection parameters, including jdbcUrl, schema, table, and so on
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
  - definition：The jdbc connection string for the relational database,search document for detail information of jdbcUrl：[MySQL doc](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)
  - necessary：true
  - data type：string
  - default：null
  <br />

- **schema**
  - definition：schema name of  database
  - necessary：false
  - data type：string
  - default：null
  <br />

- **table**
  - definition：The name of the table of the destination table.Currently, only a single table is supported, and multiple tables are supported later.
  - necessary：true
  - data type：List
  - default：null
  <br />

- **username**
  - definition：username of database
  - necessary：true
  - data type：String
  - default：null
  <br />

- **password**
  - definition：password of database
  - necessary：true
  - data type：String
  - default：null
  <br />

- **fetchSize**
  - definition：Read the data size from the database at one time. MySQL will read all the results into the memory once by default. When the amount of data is large, it may cause OOM. Setting this parameter can control the fetchSize data read each time, instead of the default Read all the data at once; enable fetchSize to meet the following requirements: the database version must be higher than 5.0.2, and the connection parameter useCursorFetch=true.
  Attention：The value of this parameter cannot be set too large, otherwise the reading will time out and the task will fail.
  - necessary：false
  - data type：int
  - default：1024
  <br />

- **where**
  - definition：Filter conditions, the reader plug-in splices SQL according to the specified column, table, and where conditions, and performs data extraction based on this SQL. In actual business scenarios, the data of the day is often selected for synchronization, and the where condition can be specified as gmt_create> time.
  - Attention：The where condition cannot be specified as limit 10. Limit is not a legal where clause of SQL.
  - necessary：false
  - data type：String
  - default：null 
  <br />

- **splitPk**
  - definition：Specifying this parameter when channell in the speed configuration is greater than 1, the Reader plug-in stitches the sql based on the number of concurrings and the fields specified by this parameter, allowing each concurrent to read different data and increasing the read rate.
  - Attention：
      - SplitPk is recommended to use the table primary key, because the table primary key is usually more uniform, so the sliced out is not easy to appear data hot spots；
      - Currently splitPk only supports integer data segmentation, and does not support other types such as floating point, string, and date. If the user specifies other non-supported types, ChunJun will report an error；
      - If the channel is greater than 1 but this parameter is not configured, the task will be set as failed.
  - necessary：false
  - data type：String 
  - default：null 
  <br />

- **queryTimeOut**
  - definition：Query timeout，Unit: second。
  - Attention：When the amount of data is large, or when querying from a view, or a custom sql query, you can specify the timeout period through this parameter.
  - necessary：false
  - data type：int 
  - default：1000
  <br />

- **customSql**
  - definition：For custom query statements, if only the specified fields cannot meet the requirements, you can specify the query sql through this parameter, which can be arbitrarily complex query statements.
  - Attention：
    - It can only be a query statement, otherwise it will cause the task to fail;
    - The fields returned by the query statement need to correspond to the fields in the column list;
    - When this parameter is specified, the table specified in the connection is invalid;
    - When specifying this parameter, column must specify specific field information, and cannot be replaced by *;
  - necessary：false
  - data type：String 
  - default：null 
  <br />

- **column**
  - definition：Need to read the field.
  - format：Support 3 formats
    <br />1.Read all fields, if there are a lot of fields, you can use the following wording：
    
    ```bash
    "column":["*"]
    ```
    2.Specify only the field name：
    
    ```
    "column":["id","name"]
    ```
    3.Specify specific information：
    
    ```json
    "column": [{
        "name": "col",
        "type": "datetime",
        "format": "yyyy-MM-dd hh:mm:ss",
        "value": "value"
    }]
    ```
  - Property description:
    - name：the name of the field
    - type：The field type can be different from the field type in the database, the program will do a type conversion
    - format：If the field is a time string, you can specify the time format and convert the field type to date format to return
    - value：If the specified field does not exist in the database, the value of value will be returned as a constant column. If the specified field exists, when the value of the specified field is null, the value will be returned as default.
  - necessary：true
  - default：null
  <br />
  
- **polling**
  - definition：Whether to enable interval polling, after enabling it, data will be periodically pulled from the database according to the pollingInterval polling interval. To enable interval polling, you need to configure the parameters pollingInterval and increColumn, and you can choose the configuration parameter startLocation. If the parameter startLocation is not configured, the maximum value of the incremental field will be queried from the database as the starting position of the poll when the task starts.
  - necessary：false
  - data type：Boolean 
  - default：false 
  <br />

- **pollingInterval**
  - definition：Polling interval, the interval of pulling data from the database, the default is 5000 milliseconds.
  - necessary：false
  - data type：long 
  - default：5000 
  <br />

- **increColumn**
  - definition：Incremental field, which can be the corresponding incremental field name, or a pure number, indicating the sequential position of the incremental field in the column (starting from 0)
  - necessary：false
  - data type：String or int 
  - default：null 
  <br />

- **startLocation**
  - definition：Incremental query start position
  - necessary：false
  - data type：String 
  - default：null 
  <br />

- **useMaxFunc**
  - definition：It is used to mark whether to save one or more pieces of data of the endLocation location, true: do not save, false (default): save, in some cases the last few data may be recorded repeatedly, this parameter can be configured as true.
  - necessary：false
  - data type：Boolean 
  - default：false 
  <br />

- **requestAccumulatorInterval**
  - definition：The interval between sending the query accumulator request.
  - necessary：false
  - data type：int 
  - default：2 
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
  - definition：table-name
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

- **scan.polling-interval**
  - definition：Interval training time.Optional(Leave blank as patch task)，default value is null.
  - necessary：false
  - data type：String 
  - default：null
  <br />

- **scan.parallelism**
  - definition：Parallelism
  - necessary：false
  - data type：String 
  - default：null
  <br />

- **scan.fetch-size**
  - definition：Each fetch size from the database.Unit: Piece
  - necessary：false
  - data type：String 
  - default：1024
  <br />

- **scan.query-timeout**
  - definition：Database connection timeout time, unit: second.
  - necessary：false
  - data type：String 
  - default：1
  <br />

- **scan.partition.column**
  - definition：The segmentation field used when multiple parallelism is enabled to read data
  - necessary：false
  - data type：String 
  - default：null
  <br />

- **scan.partition.strategy**
  - definition：Data fragmentation strategy
  - necessary：false
  - data type：String 
  - default：range
  <br />

- **scan.increment.column**
  - definition：Increment field name
  - necessary：false
  - data type：String 
  - default：null
  <br />

- **scan.increment.column-type**
  - definition：Incremental field type
  - necessary：false
  - data type：String 
  - default：null
  <br />
  
- **scan.start-location**
  - definition：The start position of the increment field, if not specified, all will be synchronized first, and then in the increment
  - necessary：false
  - data type：String 
  - default：null
  <br />

- **scan.restore.columnname**
  - definition：When check-point is turned on, the task continues with the field name of save-point/check-point. If you continue to run, it will overwrite the start position of scan.start-location, starting from the point where you continue to run.
  - necessary：false
  - data type：String 
  - default：null
  <br />

- **scan.restore.columntype**
  - definition：When check-point is turned on, the task continues from save-point/check-point field type
  - necessary：false
  - data type：String 
  - default：null
  <br />

## 5. Data Type
| Support | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| unSupport | ARRAY、MAP、STRUCT、UNION |


## 6. Profile Demo
see`chunjun-examples`directory.

