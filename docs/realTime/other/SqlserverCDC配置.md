# SqlServer配置CDC
<!-- TOC -->

- [SqlServer配置CDC](#sqlserver配置cdc)
    - [1、查询SqlServer数据库版本](#1查询sqlserver数据库版本)
    - [2、查询当前用户权限，必须为 sysadmin 固定服务器角色的成员才允许对数据库启用CDC(变更数据捕获)功能](#2查询当前用户权限必须为 sysadmin 固定服务器角色的成员才允许对数据库启用cdc变更数据捕获功能)
    - [3、查询数据库是否已经启用CDC(变更数据捕获)功能](#3查询数据库是否已经启用cdc变更数据捕获功能)
    - [4、对数据库数据库启用CDC(变更数据捕获)功能](#4对数据库数据库启用cdc变更数据捕获功能)
    - [5、查询表是否已经启用CDC(变更数据捕获)功能](#5查询表是否已经启用cdc变更数据捕获功能)
    - [6、对表启用CDC(变更数据捕获)功能](#6对表启用cdc变更数据捕获功能)
    - [7 数据库配置](#7-数据库配置)

注：SqlServer自2008版本开始支持CDC(变更数据捕获)功能，本文基于SqlServer 2017编写。

<!-- /TOC -->

#### 1、查询SqlServer数据库版本
SQL：`SELECT @@VERSION`
结果：
<div align=center>
  <img src="../../images/SqlserverCDC/Sqlserver1.png" />
</div>


#### 2、查询当前用户权限，必须为 sysadmin 固定服务器角色的成员才允许对数据库启用CDC(变更数据捕获)功能
SQL：`exec sp_helpsrvrolemember 'sysadmin'`
结果：
<div align=center>
<img src="../../images/SqlserverCDC/Sqlserver2.png" />
</div>


#### 3、查询数据库是否已经启用CDC(变更数据捕获)功能
SQL：`select is_cdc_enabled, name from  sys.databases where name = 'tudou'`
结果：
<div align=center>
<img src="../../images/SqlserverCDC/Sqlserver3.png" />
</div>
<br/>
0：未启用；1：启用


#### 4、对数据库数据库启用CDC(变更数据捕获)功能
SQL：
```sql
USE tudou  
GO  
EXEC sys.sp_cdc_enable_db  
GO  
```


重复第三步操作，确认数据库已经启用CDC(变更数据捕获)功能。

<div align=center>
<img src="../../images/SqlserverCDC/Sqlserver4.png" />
</div>


#### 5、查询表是否已经启用CDC(变更数据捕获)功能
SQL：`select name,is_tracked_by_cdc from sys.tables where name = 'test';`
结果：
<div align=center>
<img src="../../images/SqlserverCDC/Sqlserver5.png" />
</div>
0：未启用；1：启用


#### 6、对表启用CDC(变更数据捕获)功能    
SQL：
```sql
EXEC sys.sp_cdc_enable_table 
@source_schema = 'dbo', 
@source_name = 'test', 
@role_name = NULL, 
@supports_net_changes = 0;
```
source_schema：表所在的schema名称
source_name：表名
role_name：访问控制角色名称，此处为null不设置访问控制
supports_net_changes：是否为捕获实例生成一个净更改函数，0：否；1：是


重复第五步操作，确认表已经启用CDC(变更数据捕获)功能。
<div align=center>
<img src="../../images/SqlserverCDC/Sqlserver6.png" />
</div>

至此，表`test`启动CDC(变更数据捕获)功能配置完成。

#### 7 数据库配置
@supports_net_changes 需要配置

参考阅读：[https://docs.microsoft.com/zh-cn/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-2017](https://docs.microsoft.com/zh-cn/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-2017)
