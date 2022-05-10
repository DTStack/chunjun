# Oracle Lookup

## 1、Introduce

Oracle lookup,support all and lru cache<br />
all cache:All data would be loaded into memory since the program start ,which is not recommended to use in scenarios with large amount of data .<br />
lru cache:Query data asynchronously and add data to lru cache,which is recommended to use in scenarios with large amount of data.

## 2、Version Support

Oracle 9 and above

## 3、Connector name

| SQL | oracle-x |
| --- | --- |

## 4、Parameter description

- **connector**
    - Description:oracle-x
    - Required:optional
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
    - Default:none
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

- **lookup.cache-type**
    - Description:lookup table type (NONE、LRU、ALL),default value is LRU
    - Required:optional
    - Type:String
    - Default:LRU
      <br />

- **lookup.cache-period**
    - Description:Interval for loading data when the cache type is all,default value is 3600000ms
    - Required:optional
    - Type:string
    - Default:3600000
      <br />

- **lookup.cache.max-rows**
    - Description:the cache rows of lru lookup table ,default value is 10000
    - Required:optional
    - Type:string
    - Default:10000
      <br />

- **lookup.cache.ttl**
    - Description:Interval for loading data when the cache type is lru,default value is 60000ms
    - Required:optional
    - Type:string
    - Default:60000
      <br />

- **lookup.fetch-size**
    - Description:the num of data fetched from the oracle table which is used as lookup all table at a time
    - Required:optional
    - Type:string
    - Default:1000
      <br />

- **lookup.parallelism**
    - Description:the parallelism of the lookup table
    - Required:optional
    - Type:string
    - DEfault:none
      <br />

## 5、Supported data type

| Supported data type | SMALLINT、BINARY_DOUBLE、CHAR、VARCHAR、VARCHAR2、NCHAR、NVARCHAR2、INT、INTEGER、NUMBER、DECIMAL、FLOAT、DATE、RAW、LONG RAW、BINARY_FLOAT、TIMESTAMP、TIMESTAMP WITH LOCAL TIME ZONE、TIMESTAMP WITH TIME ZON、INTERVAL YEAR、INTERVAL DAY |
| :---: | :---: |
| Not supported at the moment | BFILE、XMLTYPE、Collections、BLOB、CLOB、NCLOB  |

Attention:Oracle numeric data may lose precision during conversion due to the limit of flink DecimalType's PRECISION(1~38) and SCALE(0~PRECISION)

## 六、Demo

see details in `flinkx-examples` dir of project flinkx.
