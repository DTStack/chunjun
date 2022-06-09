# SqlServer Lookup

## 1. Introduction
SqlServer Lookup table，full and asynchronous approaches are supported.<br />
Full cache: Load all dimensional table data into memory, and it is recommended that the amount of data is not used.<br />
Asynchronous caching: Query data using asynchronous means, and cache the queried data to memory using lru, which is recommended for large amounts of data.

## 2. Support Version
Microsoft SQL Server 2012 and above

## 3. Plugin Name
| SQL | sqlserver-x |
| --- | --- |

## 4. Parameter

- **connector**
   - definition：connection plugin  name
   - required：required
   - data type：String
   - value：sqlserver-x

​<br /> 

- **url**
   - definition：Use the open source jtds driver connection instead of Microsoft's official driver<br />jdbcUrl Reference documents：[jtds Reference documents](http://jtds.sourceforge.net/faq.html)
   - required：required
   - data type：String
   - default：none

<br />

- **table-name**
   - definition：table name
   - required：required
   - data type：String
   - default：none

<br />

- **schema**
   - definition：schema
   - required：optional
   - data type：String
   - default：none

​<br />

- **username**
   - definition：username
   - required：required
   - data type：String
   - default：none

​<br />

- **password**
   - definition：password
   - required：required
   - data type：String
   - default：none

​<br />

- **lookup.cache-type**
   - definition：Dimension table cache type(NONE、LRU、ALL)，default is LRU
   - required：optional
   - data type：string
   - default：LRU

<br />

- **lookup.cache-period**
   - definition：Interval for loading data when the cache type is all， default is 3600000ms(one hour)
   - required：optional
   - data type：string
   - default：3600000

<br />

- **lookup.cache.max-rows**
   - definition：the cache rows of lru lookup table ,default value is 10000
   - required：optional
   - data type：string
   - default：10000

<br />

- **lookup.cache.ttl**
   - definition：Interval for loading data when the cache type is lru,default value is 60000ms
   - required：optional
   - data type：string
   - default：60000

​<br />

- **lookup.fetch-size**
   - definition：the num of data fetched from the  oracle table which is used as lookup all table at a time
   - required：optional
   - data type：string
   - default：1000

​<br /> 

- **lookup.parallelism**
   - definition：the parallelism of the lookup table
   - required：optional
   - data type：string
   - default：none



## 5. Data type
|Supported data type | BIT、INT、SMALLINT、TINYINT、BIGINT、INT IDENTITY、REAL、FLOAT、DECIMAL、NUMERIC、CHAR、VARCHAR、VARCHAR(MAX)、TEXT、XML、NCHAR、NVARCHAR、NVARCHAR(MAX)、NTEXT、TIME、DATE、DATETIME、DATETIME2、SMALLDATETIME、DATETIMEOFFSET、TIMESTAMP、BINARY、VARBINARY、IMAGE、MONEY、SMALLMONEY、UNIQUEIDENTIFIER |
| ---| ---|
| Not supported at the moment | CURSOR、ROWVERSION、HIERARCHYID、SQL_VARIANT、SPATIAL GEOMETRY TYPE、SPATIAL GEOGRAPHY TYPE、TABLE |

## 6. Profile Demo
see `chunjun-examples` directory。
