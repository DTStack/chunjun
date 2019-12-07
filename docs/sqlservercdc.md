# SqlServer CDC读取插件（*reader）

## 1. 配置样例

```json
{
    "job": {
        "content": [{
            "reader" : {
                "parameter" : {
                  "username" : "sa",
                  "password" : "Password!",
                  "url" : "jdbc:sqlserver://kudu4:1433;databaseName=testDB",
                  "databaseName" : "testDB",
                  "tableList" : [
                    "dbo.customers",
                    "dbo.orders"
                  ],
                  "cat" : "insert,update",
                  "pavingData" : true,
                  "pollInterval" : 1000,
                  "lsn" : "00000032:00002040:0005"
                },
                "name" : "sqlservercdcreader"
             },
            "writer": {

            }
        }]
    },
    "setting": {

    }
}
```
## 2. 使用说明

使用该插件前，需要对数据库及表启用SqlServerCDC功能。具体启用过程请参照SqlServer[官方文档](https://docs.microsoft.com/zh-cn/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver15)

## 3. 参数说明

* **name**
  
  * 描述：插件名，此处填写插件名称。
  
  * 必选：是
  
  * 默认值：无
  
* **jdbcUrl**

  * 描述：SqlServer数据库的jdbc连接字符串，参考文档：[SqlServer官方文档](https://docs.microsoft.com/zh-cn/sql/connect/jdbc/overview-of-the-jdbc-driver?view=sql-server-2017)
  
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

* **tableList**
  
  * 描述：需要解析的数据表，格式为schema.table
  
  * 必选：否
  
  * 默认值：无
  
* **cat**
  
  * 描述：需要解析的数据更新类型，包括insert、update、delete三种。
  
  * 注意：以英文逗号分割的格式填写。
  
  * 必选：是
  
  * 默认值：无
  
* **pollInterval**
  
  * 描述：监听拉取SqlServer CDC数据库间隔时间。
  
  * 注意：该值越小，采集延迟时间越小，给数据库的访问压力越大。
  
  * 必选：否
  
  * 默认值：1000 
  
* **lsn**
  
  * 描述：要读取SqlServer CDC日志序列号的开始位置。
  
  * 注意：该值越小，采集延迟时间越小，给数据库的访问压力越大。
  
  * 必选：否
 
* **pavingData**
  
  * 描述：是否将解析出的json数据拍平
  
  * 示例：假设解析的表为tb1，schema为dbo，对tb1中的id字段做update操作，id原来的值为1，更新后为2，则pavingData为true时数据格式为：
    
    ```json
    {
        "type":"update",
        "schema":"dbo",
        "table":"customers",
        "lsn":"00000032:00002038:0005",
        "ingestion":1475129582923642,
        "before_id":1,
        "after_id":2
    }
    ```
    
    pavingData为false时：
    
    ```json
    {
        "type":"update",
        "schema":"dbo",
        "table":"customers",
        "lsn":"00000032:00004a38:0007",
        "ingestion":1481628798880038,
        "before":{
            "id":1
        },
        "after":{
            "id":2
        }
    }
    ```
    
    其中：ingestion是插件解析这条数据的纳秒时间，lsn是该数据变更的日志序列号
  
  * 必选：否
  
  * 默认值：false  