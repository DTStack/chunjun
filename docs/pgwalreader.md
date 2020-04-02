# PostgreSQL WAL读取插件（*reader）

## 1. 配置样例

```json
{
    "job": {
        "content": [{
            "reader" : {
                    "parameter" : {
                      "username" : "postgres",
                      "password" : "abc123",
                      "url" : "jdbc:postgresql://172.16.8.122:5432/postgres",
                      "databaseName" : "postgres",
                      "cat" : "update,insert,delete",
                      "tableList" : [
                        "changepk.test_table"
                      ],
                      "statusInterval" : 10000,
                      "lsn" : 0,
                      "slotName" : "",
                      "allowCreateSlot" : true,
                      "temporary" : true,
                      "pavingData" : true
                    },
                    "name" : "pgwalreader"
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
   1、PostgreSQL数据库版本至少为10.0及以上
   
   2、预写日志级别(wal_level)必须为logical
   
   3、该插件基于PostgreSQL逻辑复制及逻辑解码功能实现的，因此PostgreSQL账户至少拥有replication权限，若允许创建slot，则至少拥有超级管理员权限
   
   4、详细原理请参见[PostgreSQL官方文档](http://postgres.cn/docs/10/index.html)

## 3. 参数说明

* **name**
  
  * 描述：插件名，此处填写插件名称。
  
  * 必选：是
  
  * 默认值：无
  
* **jdbcUrl**

  * 描述：PostgreSQL数据库的jdbc连接字符串，参考文档：[PostgreSQL官方文档](https://jdbc.postgresql.org/documentation/head/connect.html)
  
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
  
* **statusInterval**
  
  * 描述：复制期间，数据库和使用者定期交换ping消息。如果数据库或客户端在配置的超时时间内未收到ping消息，则复制被视为已停止，并且将引发异常，并且数据库将释放资源。在PostgreSQL中，ping超时由属性wal_sender_timeout配置（默认= 60秒）。可以将pgjdc中的复制流配置为在需要时或按时间间隔发送反馈（ping）。建议比配置的wal_sender_timeout更频繁地向数据库发送反馈（ping）。在生产环境中，我使用等于wal_sender_timeout / 3的值。它避免了网络潜在的问题，并且可以在不因超时而断开连接的情况下传输更改。
  
  * 必选：否
  
  * 默认值：2000 
  
* **lsn**
  
  * 描述：要读取PostgreSQL WAL日志序列号的开始位置。
  
  * 必选：否
  
  * 默认值：0
  
* **slotName**
  
  * 描述：复制槽名称，根据该值去寻找或创建复制槽
  
  * 注意：当allowCreateSlot为false时，该值不能为空
  
  * 必选：否
  
  * 默认值：无
  
* **allowCreateSlot**
  
  * 描述：是否允许创建复制槽
  
  * 必选：否
  
  * 默认值：true

* **temporary**
  
  * 描述：复制槽是否为临时性的，true：是；false：否。
  
  * 必选：否

  * 默认值：true
 
* **pavingData**
  
  * 描述：是否将解析出的json数据拍平
  
  * 示例：假设解析的表为tb1，schema为dbo，对tb1中的id字段做update操作，id原来的值为1，更新后为2，则pavingData为true时数据格式为：
    
    ```json
    {
        "type":"update",
        "schema":"dbo",
        "table":"customers",
        "lsn":207967352,
        "ts": 1576487525488,
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
        "lsn":207967352,
        "ts": 1576487525488,
        "ingestion":1481628798880038,
        "before":{
            "id":1
        },
        "after":{
            "id":2
        }
    }
    ```
    
    其中：ts是数据库中数据的变更时间，ingestion是插件解析这条数据的纳秒时间，lsn是该数据变更的日志序列号
  
  * 必选：否
  
  * 默认值：false  

