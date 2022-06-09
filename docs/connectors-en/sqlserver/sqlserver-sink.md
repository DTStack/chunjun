# SqlServer Source

## 1. Introduce
SqlServer Sink support to write data to database SQLServer

## 2. Version Support
Microsoft SQL Server 2012 and above

## 3、Connector name
| Sync | sqlserverwriter、sqlserversink |
| --- | --- |
| SQL | sqlserver-x |

## 4、Parameter description

### 1.Sync

- **connection**
  - Description:param for Database connection,including jdbcUrl、schema、table and so on
    - Required:required
    - Type:List
    - Default:none
    
```json
"connection": [{
     "jdbcUrl": "jdbc:jtds:sqlserver://0.0.0.1:1433;DatabaseName=DTstack",
     "table": ["table"],
  	 "schema":"public"
    }]
```



<br />

- **jdbcUrl**
   - Description：Use the open source jtds driver connection instead of Microsoft's official driver<br />jdbcUrlReference documents：[jtds Reference documents](http://jtds.sourceforge.net/faq.html)
   - Required：required
   - Type：String
   - Default：none


<br />

- **schema**
  - Description:Database schema
  - Required:optional
  - Type:string
  - Default: none
    <br />

- **table**
  - Description: sqlserver table name, only support one table in a single work at the moment.
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

​<br />

- **postSql**
  - Description:the sql executed  after writing data into the destination table
  - Required:optional
  - Type:List
  - Default:none
    <br />

<br/>

- **writeMode**
  - Description:the mode of  writing data, insert into or merge into
  - Required:required
  - All options:insert/update
  - Type:String
  - Default:insert

<br />

- **withNoLock**
   - Description：add the sql with(nolock)
   - Required：optional
   - Type：Boolean
   - Default：false

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

- **batchSize**
  - Description:The number of records submitted in batch at one time. This value can greatly reduce the number of network interactions between chunjun and the database and improve the overall throughput,Setting this value too large may cause the chunjun process to run oom
  - Required:optional
  - Type:int
  - Default:1024



### 2.SQL

- **connector**
   - Description：connector type
   - Required：required
   - Type：String
   - value：sqlserver-x

<br/>

- **url**
   - Description：Use the open source jtds driver connection instead of Microsoft's official driver
   - Required：required
   - Type：String
   - Default：none

​<br />

- **table-name**
  - Description: table name
  - Required:required
  - Type:String
  - Default:none
    <br />

- **schema**
   - Description：schema
   - Required：optional
   - Type：String
   - Default：none

<br/>

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

<br/>

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

<br/>

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

- **sink.parallelism**
  - Description:the parallelism of sink operator
  - Required:optional
  - Type:String
  - Default:none
    <br />



## 5. Supported data type
|Supported data type | BIT、INT、SMALLINT、TINYINT、BIGINT、INT IDENTITY、REAL、FLOAT、DECIMAL、NUMERIC、CHAR、VARCHAR、VARCHAR(MAX)、TEXT、XML、NCHAR、NVARCHAR、NVARCHAR(MAX)、NTEXT、TIME、DATE、DATETIME、DATETIME2、SMALLDATETIME、DATETIMEOFFSET、TIMESTAMP、BINARY、VARBINARY、IMAGE、MONEY、SMALLMONEY、UNIQUEIDENTIFIER |
| ---| ---|
| Not supported at the moment | CURSOR、ROWVERSION、HIERARCHYID、SQL_VARIANT、SPATIAL GEOMETRY TYPE、SPATIAL GEOGRAPHY TYPE、TABLE |
## 6. Demo
see details in`chunjun-examples` directory.
