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

使用该插件前，需要对数据库及表启用SqlServerCDC功能。

   * 1、查询SqlServer数据库版本
        * SqlServer自2008版本开始支持CDC(变更数据捕获)功能，但若想使用SqlServer CDC实时采集插件需SqlServer版本为2017及以上
```sql
    SELECT @@VERSION
```
   * 2、查询当前用户权限，必须为 sysadmin 固定服务器角色的成员才允许对数据库启用CDC(变更数据捕获)功能
```sql
    exec sp_helpsrvrolemember 'sysadmin'
```   
   * 3、查询数据库是否已经启用CDC(变更数据捕获)功能
        * 0：未启用；1：启用
```sql
    select is_cdc_enabled, name from  sys.databases where name = 'name'
```   
   * 4、对数据库数据库启用CDC(变更数据捕获)功能
```sql
    EXEC sys.sp_cdc_enable_db  
```   
   * 5、对表启用CDC(变更数据捕获)功能
        * source_schema：表所在的schema名称
        * source_name：表名
        * role_name：访问控制角色名称，此处为null不设置访问控制
        * supports_net_changes：是否为捕获实例生成一个净更改函数，0：否；1：是
```sql
    EXEC sys.sp_cdc_enable_table 
    @source_schema = 'dbo', 
    @source_name = 'test', 
    @role_name = NULL, 
    @supports_net_changes = 0;
```   
   * 6、查询表是否已经启用CDC(变更数据捕获)功能
       * 0：未启用；1：启用
```sql
    select name,is_tracked_by_cdc from sys.tables where name = 'test';
```   

具体启用过程请参照SqlServer[官方文档](https://docs.microsoft.com/zh-cn/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver15)

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
  
  * 描述：需要解析的数据表，表必须已启用CDC，格式为schema.table
  
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