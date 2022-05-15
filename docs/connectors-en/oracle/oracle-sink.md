# Oracle Sink

## 1、Introduce
oracle sink

## 2、Version Support
Oracle 9 and above


## 3、Connector name
| Sync | oraclesink、oraclewriter |
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
  - Default:none
    <br />

- **schema**
  - Description:Database schema
  - Required:optional
  - Type:string
  - Default:oracle user name
    <br />

- **table**
  - Description: oracle table name, only support one table in a single work at the moment.
  - Required:required
  - Type:List
  - Default:none
    <br />

- **username**
  - Description: user name 
  - Required:required
  - Type:String
  - Default:none
    <br />

- **password**
  - Description: password
  - Required:required
  - Type:String
  - Default:none
    <br />

- **column**
  - Description:the fields to be written to the destination table,which is separated by English commas.for example: "column": ["id","name","age"]
  - Required:required
  - Type:List
  - Default:none
    <br />

- **fullcolumn**
  - Description:All fields in the destination table ,which is separated by English commas.for example: "column": ["id","name","age","hobby"],if not configured, it will be obtained in the system table
  - Required:optional
  - Type:List
  - Default:none
    <br />

- **preSql**
  - Description:the sql executed  before writing data into the destination table
  - Required:optional
  - Type:List
  - Default:none
    <br />

- **postSql**
  - Description:the sql executed  after writing data into the destination table
  - Required:optional
  - Type:List
  - Default:none
    <br />

- **writeMode**
  - Description:the mode of  writing data, insert into or merge into
  - Required:required
  - All options:insert/update
  - Type:String
  - Default:insert
    <br />

- **batchSize**
  - Description:The number of records submitted in batch at one time. This value can greatly reduce the number of network interactions between chunjun and the database and improve the overall throughput,Setting this value too large may cause the chunjun process to run oom
  - Required:optional
  - Type:int
  - Default:1024
    <br />

- **updateKey**
  - Description:When the write mode is update, you need to specify the value of this parameter as the unique index field
  - attention:
    - If this parameter is empty and the write mode is update, the application will automatically obtain the unique index in the database;
    - If the data table does not have a unique index, but the required write mode is configured as update and, the application will write data in the way of insert;
  - Required:optional
  - Type:Map<String,List>
    - for example:"updateKey": {"key": ["id"]}
  - Default:none
    <br />
    
- **semantic**
  - Description:sink operator support phase two commit
  - attention:
    -If this parameter is blank, phase two commit is not enabled by default,which means sink operators do not support exactly-once semantics
    -Currently only supported exactly-once and at-least-once 
  - Required:optional
  - Type:String
    - for example:"semantic": "exactly-once"
  - Default:at-least-once
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
  - Description: table name
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

- **sink.buffer-flush.max-rows**
  - Description:Number of data pieces written in batch
  - Required:optional
  - Type:String
  - Default:1024
    <br />

- **sink.buffer-flush.interval**
  - Description:Batch write interval,Unit: ms
  - Required:optional
  - Type:String
  - Default:10000
    <br />

- **sink.all-replace**
  - Description: whether to replace all data in the database
  - Required:optional
  - Type:String
  - Default:false
    <br />

- **sink.parallelism**
  - Description:the parallelism of sink operator
  - Required:optional
  - Type:String
  - Default:none
    <br />
    
- **sink.semantic**
  - Description:sink operator support phase two commit
  - attention:
    -If this parameter is blank, phase two commit is not enabled by default,which means sink operators do not support exactly-once semantics;
    -Currently only supported exactly-once and at-least-once 
  - Required:optional
  - Type:String
    - for example:"semantic": "exactly-once"
  - Default:at-least-once
<br />

## 5、Supported data type
| Supported data type | SMALLINT、BINARY_DOUBLE、CHAR、VARCHAR、VARCHAR2、NCHAR、NVARCHAR2、INT、INTEGER、NUMBER、DECIMAL、FLOAT、DATE、RAW、LONG RAW、BINARY_FLOAT、TIMESTAMP、TIMESTAMP WITH LOCAL TIME ZONE、TIMESTAMP WITH TIME ZON、INTERVAL YEAR、INTERVAL DAY |
| :---: | :---: |
| Not supported at the moment | BFILE、XMLTYPE、Collections、BLOB、CLOB、NCLOB  |

Attention:Oracle numeric data may lose precision during conversion due to the limit of  flink DecimalType's PRECISION(1~38) and  SCALE(0~PRECISION)



## 6、Demo
see details in `chunjun-examples` dir of project chunjun.
