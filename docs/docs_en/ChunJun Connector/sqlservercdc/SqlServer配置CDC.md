# SqlServer 配置 CDC

注：SqlServer 自 2008 版本开始支持 CDC(变更数据捕获)功能，本文基于 SqlServer 2017 编写。

#### 1、查询 SqlServer 数据库版本

SQL：`SELECT @@VERSION`
结果：

![image](/doc/SqlserverCDC/Sqlserver1.png)

#### 2、查询当前用户权限，必须为  sysadmin  固定服务器角色的成员才允许对数据库启用 CDC(变更数据捕获)功能

SQL：`exec sp_helpsrvrolemember 'sysadmin'`
结果：

![image](/doc/SqlserverCDC/Sqlserver2.png)

#### 3、查询数据库是否已经启用 CDC(变更数据捕获)功能

SQL：`select is_cdc_enabled, name from sys.databases where name = 'tudou'`
结果：

![image](/doc/SqlserverCDC/Sqlserver3.png)

0：未启用；1：启用

#### 4、对数据库数据库启用 CDC(变更数据捕获)功能

SQL：

```sql
USE
tudou
GO
EXEC sys.sp_cdc_enable_db
GO
```

重复第三步操作，确认数据库已经启用 CDC(变更数据捕获)功能。

![image](/doc/SqlserverCDC/Sqlserver4.png)

#### 5、查询表是否已经启用 CDC(变更数据捕获)功能

SQL：`select name,is_tracked_by_cdc from sys.tables where name = 'test';`
结果：

![image](/doc/SqlserverCDC/Sqlserver5.png)

0：未启用；1：启用

#### 6、对表启用 CDC(变更数据捕获)功能     

SQL：

```sql
EXEC sys.sp_cdc_enable_table
@source_schema = 'dbo',
@source_name = 'test',
@role_name = NULL,
@supports_net_changes = 0;
```

source_schema：表所在的 schema 名称 source_name：表名 role_name：访问控制角色名称，此处为 null 不设置访问控制
supports_net_changes：是否为捕获实例生成一个净更改函数，0：否；1：是

重复第五步操作，确认表已经启用 CDC(变更数据捕获)功能。

![image](/doc/SqlserverCDC/Sqlserver6.png)

至此，表`test`启动 CDC(变更数据捕获)功能配置完成。

#### 7、确认 CDC agent 是否正常启动

```sql
EXEC master.dbo.xp_servicecontrol N'QUERYSTATE', N'SQLSERVERAGENT'
```

![image](/doc/SqlserverCDC/Sqlserver16.png)

如显示上图状态，需要启动对应的 agent.

**Windows 环境操作开启 CDC agent**
点击 下图位置代理开启

![image](/doc/SqlserverCDC/Sqlserver17.png)>

**重新启动数据库**

![image](/doc/SqlserverCDC/Sqlserver18.png)

**再次查询 agent 状态，确认状态变更为 running**

![image](/doc/SqlserverCDC/Sqlserver19.png)

至此，表`test`启动 CDC(变更数据捕获)功能配置完成。

**docker 环境操作开启 CDC agent**

**开启 mssql-server 的代理服务**\_

```shell
docker exec -it sqlserver bash
/opt/mssql/bin/mssql-conf set sqlagent.enabled true
docker stop sqlserver
docker start sqlserver
```

参考阅读：[https://docs.microsoft.com/zh-cn/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-2017](https://docs.microsoft.com/zh-cn/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-2017)
