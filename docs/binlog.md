# MySQL binlog读取插件（*reader）

## 1.首先给莫个用户赋权，有读binglog的权限

     GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT
     ON *.* TO 'xxx'@'%' IDENTIFIED BY 'xxx';

## 2. 配置样例

```json
{
    "job": {
        "content": [{
            "reader": {
                "parameter": {
                    "jdbcUrl" : "jdbc:mysql://127.0.0.1:3306/test?charset=utf8",
                    "username" : "username"，
                    "password" : "password",
                    "host" : "127.0.0.1",
                    "port": 3306,
                    "table" : [ "test_sink" ],
                    "filter" : "",
                    "cat" : "insert,update,delete",
                    "start" : {
                        "journalName" : "bin.000004",
                        "timestamp" : 123123
                    },
                    "pavingData" : false,
                    "bufferSize" : 1024
                },
                "name": "binlogreader"
            },
            "writer": {

            }
        }]
    },
    "setting": {

    }
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名，此处填写插件名称。
  
  * 必选：是
  
  * 默认值：无

* **jdbcUrl**
  
  * 描述：MySQL数据库的jdbc连接字符串，参考文档：[Mysql官方文档](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)
  
  * 必选：是
  
  * 默认值：无 

* **username**
  
  * 描述：数据源的用户名 
  
  * 必选：是 
  
  * 默认值：无

* **password**
  
  * 描述：数据源指定用户名的密码 
  
  * 必选：是
  
  * 默认值：无

* **host**
  
  * 描述：启动MySQL slave的机器ip。
  
  * 必选：是
  
  * 默认值：无

* **port**
  
  * 描述：启动MySQL slave的端口
  
  * 必选：否
  
  * 默认值：3306

* **table**
  
  * 描述：需要解析的数据表。
  
  * 注意：指定此参数后filter参数将无效。
  
  * 必选：否
  
  * 默认值：无

* **filter**
  
  * 描述：过滤表名的Perl正则表达式。
  
  * 例子：
    
    * 所有表：.*   or  .*\\..*
    
    * canal schema下所有表： canal\\..*
    
    * canal下的以canal打头的表：canal\\.canal.*
    
    * canal schema下的一张表：canal\\.test1
  
  * 必选：否
  
  * 默认值：无

* **cat**
  
  * 描述：需要解析的数据更新类型，包括insert、update、delete三种。
  
  * 注意：以英文逗号分割的格式填写。
  
  * 必选：否
  
  * 默认值：null

* **start**
  
  * 描述：要读取的binlog文件的开始位置。
  
  * 参数：
    
    * journalName：采集起点按文件开始时的文件名称；
    
    * timestamp：采集七点按时间开始时的时间戳；
  
  * 默认值：无

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

* **bufferSize**
  
  * 描述：并发缓存大小
  
  * 注意：必须为2的幂
  
  * 必选：否
  
  * 默认值：1024
