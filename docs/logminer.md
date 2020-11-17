# Kudu读取插件（kudureader）

## 1. 配置样例

```
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "jdbcUrl": "jdbc:oracle:thin:@127.0.0.1:1521:xe",
            "username": "kminer",
            "password": "kminerpass",
            "listenerTables": "SCHEMA1.*",
            "listenerOperations": "UPDATE,INSERT,DELETE",

            "startSCN": "482165",
            "readPosition": "current",
            "startTime": 1576540477000,
            "pavingData": true,
            "queryTimeout": 20000
          },
          "name": "oraclelogminerreader"
        },
        "writer": {
          "parameter": {
            "print": true
          },
          "name": "streamwriter"
        }
      }
    ],
    "setting": {
      "errorLimit": {
        "record": 100
      },
      "speed": {
        "bytes": 1048576,
        "channel": 1
      }
    }
  }
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名，此处填写插件名称，oraclelogminerreader。
  
  * 必选：是 
  
  * 默认值：无 

* **jdbcUrl**
  
  * 描述：Oracle连接url。
  
  * 必选：是
  
  * 默认值：无

* **username**
  
  * 描述： 用户名。
  
  * 必选：是 
  
  * 默认值：无 

* **password**
  
  * 描述： 密码。
  
  * 必选：是 
  
  * 默认值：无   

* **listenerTables**
  
  - 描述： 要监听的schema和table，使用正则进行过滤。
  
  - 必选：否
  
  - 默认值：无

* **queryTimeout**
  
  - 描述： 查询超时时间
  
  - 必选：否
  
  - 默认值：无
  
* **listenerOperations**
  
  - 描述： 要监听的事件，可多选：UPDATE、INSERT、DELETE
  
  - 必选：否
  
  - 默认值：UPDATE,INSERT,DELETE

* **readPosition**
  
  * 描述：读取位置类型，可选：all、current、time、scn
  
  * 必选：否
  
  * 默认值：current
  
  * 类型说明：
    
    * all：读取全部的事件；
    
    * current：从任务开始运行时采集事件；
    
    * time：从指定的时间开始采集，使用此类型时需要提供时间配置"startTime"，格式为毫秒级的时间戳；
    
    * scn：从指定的scn开始采集，使用此类型时需要提供scn配置"scn"；

* **pavingData**
  
  * 描述：是否将解析出的json数据拍平
  
  * 示例：假设解析的表为tb1,数据库为test，对tb1中的id字段做update操作，id原来的值为1，更新后为2，则pavingData为true时数据格式为：
    
    ```json
    {
        "type":"update",
        "schema":"test",
        "table":"tb1",
        "ts":1231232,
        "ingestion":123213,
        "before_id":1,
        "after_id":2
    }
    ```
    
    pavingData为false时：
    
    ```json
    {
        "message":{
             "type":"update",
             "schema":"test",
             "table":"tb1",
             "ts":1231232,
             "ingestion":123213,
             "before_id":{
                 "id":1
             },
             "after_id":{
                 "id":2
             }
        }
    }
    ```
    
    其中”ts“是数据变更时间，ingestion是插件解析这条数据的纳秒时间
  
  * 必选：否
  
  * 默认值：false

## 3.数据库配置

数据库必须处于archivelog模式，并且必须启用补充日志记录，配置过程如下：

```sql
    sqlplus / as sysdba    
    SQL>shutdown immediate
    SQL>startup mount
    SQL>alter database archivelog;
    SQL>alter database open;
    SQL>alter database add supplemental log data (all) columns;
```

连接数据库的用户必须有对应的权限，如果用户是 DBA角色，则不用进行设置，配置过程如下：

```sql
    create role logmnr_role;
    grant create session to logmnr_role;
    grant  execute_catalog_role,select any transaction ,select any dictionary to logmnr_role;
    create user kminer identified by kminerpass;
    grant  logmnr_role to kminer;
    alter user kminer quota unlimited on users;
```
