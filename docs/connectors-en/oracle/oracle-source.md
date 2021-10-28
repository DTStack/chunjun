# Oracle Source

## 1、Introduce
read data from oracle with batch model

## 2、Version Support
Oracle 9 and above


## 3、Connector name
| Sync | oraclesource、oraclereader |
| --- | --- |
| SQL | oracle-x |


## 4、Parameter description
### 1、Sync
- **connection**
  - Description:param for Database connection,including jdbcUrl、schema、table and so on
  - Required:required
  - Type:List
  - Default:none
    ```text
    "connection": [{
     "jdbcUrl": ["jdbc:oracle:thin:@0.0.0.1:1521:orcl"],
     "table": ["table"],
     "schema":"public"
    }]
    ```
 <br />

- **jdbcUrl**
  - Description:jdbc url,See for details[Oracle](http://www.oracle.com/technetwork/database/enterprise-edition/documentation/index.html)
  - Required:required
  - Type:string
  - Default:user name
    <br />

- **schema**
  - Description:Database schema
  - Required:optional
  - Type:string
  - Default:none
    <br />

- **table**
  - Description:oracle table name, only support one table in a single work at the moment
  - Required:required
  - Type:List
  - Default:none
    <br />

- **username**
  - Description:user name 
  - Required:required
  - Type:String
  - Default:none
    <br />

- **password**
  - Description:password
  - Required:required
  - Type:String
  - Default:none
    <br />

- **fetchSize**
  - Description:the num of records fetching from Oracle at one time,the default value is 1024.When fetchsize is set too small, frequent data reading will affect query speed and database pressure. When fetchsize is set too large, it may cause oom when the amount of data is large. Setting this parameter can control fetchsize pieces of data to be read each time.
    Attention:The value of this parameter cannot be set too large, otherwise it will read timeout and cause the task to fail.
  - Required:optional
  - Type:int
  - Default:1024
    <br />

- **where**
  - Description:query condition,readerThe plugin splices SQL according to the specified column, table and where conditions,In the actual business scenario, the data of the current day is often selected for synchronization, and the where condition can be specified as GMT_ create > time.
  - Attention:The where condition cannot be specified as limit . Limit is not a legal where clause of SQL.
  - Required:optional
  - Type:String
  - Default:none
    <br />

- **splitPk**
  - Description:When the channel in the speed configuration is greater than 1, this parameter is specified. The reader plug-in splices SQL according to the number of concurrencies and the fields specified by this parameter, so that each concurrency can read different data and improve the reading rate.
  - Attention:
    - It is recommended that splitpk use the table primary key, because the table primary key is usually uniform, so the segmented fragments are not prone to data hot spots.
    - At present, splitpk only supports integer data segmentation and does not support other types such as floating point, string and date. If the user specifies other unsupported types, flinkx will report an error.
    - If the channel is greater than 1 but this parameter is not configured, the task will be set as failed
  - Required:optional
  - Type:String
  - Default:none
    <br />

- **queryTimeOut**
  - Description:Query timeout, unit seconds
  - Attention:When there is a large amount of data, or query from the view, or custom SQL query, you can specify the timeout through this parameter.
  - Required:optional
  - Type:int
  - Default:1000
    <br />

- **customSql**
  - Description:For user-defined query statements, if only specified fields cannot meet the requirements, you can specify the SQL of the query through this parameter, which can be any complex query statement.
  - Attention:
    - Only query statements can be used, otherwise the task will fail;
    - The fields returned by the query statement need to correspond to the fields in the column list;
    - When specifying this parameter, column must specify specific field information and cannot be replaced by * sign;
  - Required:optional
  - Type:String
  - Default:none
    <br />

- **column**
  - Description:Fields to read.
  - Format:Three formats are supported
    <br />1.Read all fields. If there are many fields, you can use the following writing method:
    ```bash
    "column":["*"]
    ```
    2.Specify only field names:
    ```
    "column":["id","name"]
    ```
    3.Specify specific information:
    ```json
    "column": [{
        "name": "col",
        "type": "datetime",
        "format": "yyyy-MM-dd hh:mm:ss",
        "value": "value"
    }]
    ```
  - Attribute description:
    - name:Field name
    - type:Field type,It can be different from the field type in the database. The program will make a type conversion
    - format:If the field is a time string, you can specify the format of time and convert the field type to date format
    - value:If the specified field does not exist in the database, the value will be returned as a constant column. If the specified field exists, when the value of the specified field is null, the value will be returned as default
  - Required:required
  - Default:none
    <br />

- **polling**
  - Description:Whether to enable interval polling. After it is enabled, data will be periodically pulled from the database according to the pollinginterval polling interval. To enable interval polling, you also need to configure the parameters pollinginterval and increcolumn. You can select the configuration parameter startlocation. If the parameter startlocation is not configured, the maximum value of the increment field will be queried from the database as the starting position of polling when the task is started.
  - Required:optional
  - Type:Boolean
  - Default:false
    <br />

- **pollingInterval**
  - Description:Polling interval: the interval between pulling data from the database. The default is 5000 milliseconds.
  - Required:optional
  - Type:long
  - Default:5000
    <br />    

- **increColumn**
  - Description:The incremental field can be the corresponding incremental field name or a pure number, indicating the sequential position of the incremental field in the column (starting from 0)
  - Required:optional
  - Type:String或int
  - Default:none
    <br />

- **startLocation**
  - Description:Start position of incremental query
  - Required:optional
  - Type:String
  - Default:none
    <br />

- **useMaxFunc**
  - Description:Used to mark whether to save one or more pieces of data at endlocation. True: do not save, false (default): save. In some cases, the last few pieces of data may be repeatedly recorded. You can configure this parameter to true
  - Required:optional
  - Type:Boolean
  - Default:false
    <br />

- **requestAccumulatorInterval**
  - Description:The interval between sending query accumulator requests
  - Required:optional
  - Type:int
  - Default:2
    <br />

### 2、SQL
- **connector**
  - Description:oracle-x
  - Required:required
  - Type:String
  - Default:none
    <br />

- **url**
  - Description:jdbc:oracle:thin:@0.0.0.1:1521:orcl
  - Required:required
  - Type:String
  - Default:none
    <br />

- **table-name**
  - Description:table name
  - Required:required
  - Type:String
  - Default:none:
    <br />

- **username**
  - Description:username
  - Required:required
  - Type:String
  - Default:none
    <br />

- **password**
  - Description:password
  - Required:required
  - Type:String
  - Default:none
    <br />

- **scan.polling-interval**
  - Description:Interval rotation training time. It is not required (it is not filled in as an offline task).
  - Required:optional
  - Type:String
  - Default:none
    <br />

- **scan.parallelism**
  - Description:parallelism
  - Required:optional
  - Type:String
  - Default:none
    <br />

- **scan.fetch-size**
  - Description:Size of each fetch from the database.
  - Required:optional
  - Type:String
  - Default:1024
    <br />

- **scan.query-timeout**
  - Description:The size of each fetch from the database, unit: database connection timeout, unit: seconds.
  - Required:optional
  - Type:String
  - Default:1
    <br />

- **scan.partition.column**
  - Description:The segmentation field read by multiple parallelism must be set under multiple parallelism
  - Required:optional
  - Type:String
  - Default:none
    <br />

- **scan.partition.strategy**
  - Description:Data fragmentation strategy
  - Required:optional
  - Type:String
  - Default:range
    <br />

- **scan.increment.column**
  - Description:Incremental field name
  - Required:optional
  - Type:String
  - Default:none
    <br />

- **scan.increment.column-type**
  - Description:Incremental field type
  - Required:optional
  - Type:String
  - Default:none
    <br />

- **scan.start-location**
  - Description:The start position of the increment field. If it is not specified, all are synchronized first, and then in the increment field
  - Required:optional
  - Type:String
  - Default:none
    <br />

- **scan.restore.columnname**
  - Description:When CP is enabled, the task starts from the SP / CP continuation field name. If you continue, the start position of scan.start-location will be overwritten, starting from the continuation point
  - Required:optional
  - Type:String
  - Default:none
    <br />

- **scan.restore.columntype**
  - Description:When CP is enabled,Task continuation field type from SP / CP
  - Required:optional
  - Type:String
  - Default:none
    <br />

## 5、Supported data type
| Supported data type | SMALLINT、BINARY_DOUBLE、CHAR、VARCHAR、VARCHAR2、NCHAR、NVARCHAR2、INT、INTEGER、NUMBER、DECIMAL、FLOAT、DATE、RAW、LONG RAW、BINARY_FLOAT、TIMESTAMP、TIMESTAMP WITH LOCAL TIME ZONE、TIMESTAMP WITH TIME ZON、INTERVAL YEAR、INTERVAL DAY |
| :---: | :---: |
| Not supported at the moment | BFILE、XMLTYPE、Collections、BLOB、CLOB、NCLOB  |

Attention:Oracle numeric data may lose precision during conversion due to the limit of  flink DecimalType's PRECISION(1~38) and  SCALE(0~PRECISION)
