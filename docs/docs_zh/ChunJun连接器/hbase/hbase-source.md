# Hbase Source
## 1. Introduce
Hbase Source


## 2. Version Support
hbase1.4


## 3. Connector Name
| Sync | hbasesource、hbasereader |
| --- |-------------------------|
| SQL | hbase14-x               |



## 4. Parameter description
### 1、SYNC


- **table**
  - 描述：表名
  - 必选：是
  - 类型：String
  - 默认值：无


- **startRowkey**
    - 描述：rowKey起始点
    - 必选：否
    - 类型：String
    - 默认值：无



- **endRowkey**
    - 描述：rowKey结束点
    - 必选：否
    - 类型：String
    - 默认值：无



- **isBinaryRowkey**
    - 描述：rowkey是否是BytesBinary
    - 必选：否
    - 类型：Boolean
    - 默认值：false


- **scanCacheSize**
    - 描述：客户端rpc每次fetch最大行数
    - 必选：否
    - 类型：Long
    - 默认值：1000
  
  

- **encoding**
    - 描述：编码
    - 必选：否
    - 类型：string
    - 默认值：utf-8



- **hbaseConfig**
    - 描述：hbase-site里的相关配置 以及 kerberos相关配置
    - 必选：是
    - 类型：Map
    - 默认值：无



- **column**
  - 描述：需要读取的列族。
  - 属性说明:
    - name：字段名称
    - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
  - 必选：是
  - 字段类型：List
  - 默认值：无

### 2、SQL

- **connector**
  - 描述：hbase14-x
  - 必选：是
  - 参数类型：String
  - 默认值：无
    <br />


- **table-name**
  - 描述：表名
  - 必选：是
  - 参数类型：String
  - 默认值：无：
    <br />


- **properties.zookeeper.znode.parent**
  - 描述：hbase在zk的路径
  - 必选：否
  - 参数类型：string
  - 默认值：/hbase
    <br />


- **properties.zookeeper.quorum**
  - 描述：zk地址
  - 必选：是
  - 参数类型：String
  - 默认值：无
    <br />


- **null-string-literal**
  - 描述：空值字符串代替
  - 必选：否
  - 默认值："null"
    <br />



- **security.kerberos.principal**
  - 描述：kerberos的principal
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />


- **security.kerberos.keytab**
  - 描述：kerberos的keytab文件路径
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />



- **security.kerberos.krb5conf**
  - 描述：kdc的krb5conf配置文件
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />


## 5. Data Type

|     是否支持     |                                                                                                                                            类型名称                                                                                                                                             |
| :--------------: |:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|       支持       | BOOLEAN、TINYINT、INT8、UINT8、SMALLINT、UINT16、INT16、INTEGER、INTERVALYEAR、INTERVALQUARTER、INTERVALMONTH、INTERVALWEEK、INTERVALDAY、INTERVALHOUR、INTERVALMINUTE INTERVALSECOND、INT32、INT、UINT32、 UINT64、 INT64、 BIGINT、 FLOAT、FLOAT32、 DECIMAL、 DECIMAL32、DECIMAL64、DECIMAL128、 DEC、DOUBLE、FLOAT64、UUID、COLLECTION、BLOB、LONGTEXT、TINYTEXT、TEXT、CHAR、MEDIUMTEXT、TINYBLOB、MEDIUMBLOB、LONGBLOB、BINARY、STRUCT、VARCHAR、STRING、ENUM8、ENUM16、FIXEDSTRING、NESTED、DATE、TIME、TIMESTAMP、DATETIME、NOTHING、NULLABLE、NULL |
|     暂不支持     |                                                                                                                                                                                                                                                                 |
| 仅在 Sync 中支持 |                                                                                                                                                                                                                                                                            |


## 6. Example
The details are in chunjun-examples dir.
