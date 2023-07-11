# Oracle 配置 LogMiner

注意：

1、某个 Oracle 数据源能同时运行的任务数量取决于该 Oracle 的内存大小

2、若数据量太大导致日志组频繁切换需要增加日志组数量，增大单个日志组存储大小

## 一、Oracle 10g(单机版)

### 1、查询 Oracle 版本信息，这里配置的是`Oracle 10g`

```sql
--查看oracle版本
select *
from v$version;
```

![image](/doc/LogMiner/LogMiner1.png)

本章 Oracle 的版本如上图所示。

### 2、通过命令行方式登录 Oracle，查看是否开启日志归档

```sql
--查询数据库归档模式
archive
log list;
```

![image](/doc/LogMiner/LogMiner2.png)

图中显示`No Archive Mode`表示未开启日志归档。

### 3、开启日志归档，开启日志归档需要重启数据库，请注意

#### a、配置归档日志保存的路径

根据自身环境配置归档日志保存路径，需要提前创建相应目录及赋予相应访问权限

```shell
# 创建归档日志保存目录
mkdir -p /data/oracle/archivelog

# 进入Oracle目录
cd $ORACLE_HOME

# 查看Oracle权限组，本章权限组如下图所示
ls -l

# 对归档日志保存目录赋予相应权限
chown -R 下图中的用户名:下图中的组名 /data/oracle/
```

![image](/doc/LogMiner/LogMiner3.png)

```sql
--配置归档日志保存的路径
alter
system set log_archive_dest_1='location=/data/oracle/archivelog' scope=spfile;
```

#### b、关闭数据库

```sql
shutdown
immediate;
startup
mount;
```

#### c、开启日志归档

```sql
--开启日志归档
alter
database archivelog;
```

#### d、开启扩充日志

```sql
--开启扩充日志
alter
database add supplemental log data (all) columns;
```

#### e、开启数据库

```sql
alter
database open;
```

再次查询数据库归档模式，`Archive Mode`表示已开启归档模式，`Archive destination`表示归档日志储存路径。

![image](/doc/LogMiner/LogMiner4.png)

### 4、配置日志组

#### a、查询默认日志组信息

```sql
SELECT *
FROM v$log;
```

![image](/doc/LogMiner/LogMiner5.png)

如上图所示，日志组的默认数量为 2 组，大小为 4194304/1024/1024 = 4MB，这意味着日志大小每达到 4MB 就会进行日志组的切换，切换太过频繁会导致查询出错，因此需要增加日志组数量及大小。

#### b、查询日志组储存路径

```sql
SELECT *
FROM v$logfile;
```

![image](/doc/LogMiner/LogMiner6.png)

如上图所示，默认路径为`/usr/lib/oracle/xe/app/oracle/flash_recovery_area/XE/onlinelog/`。

#### c、新增日志组与删除原有日志组

请与 DBA 联系，决定是否可以删除原有日志组。

```sql
--增加两组日志组
alter
database add logfile group 3 ('/usr/lib/oracle/xe/app/oracle/flash_recovery_area/XE/onlinelog/redo3.log') size 200m;
alter
database add logfile group 4 ('/usr/lib/oracle/xe/app/oracle/flash_recovery_area/XE/onlinelog/redo4.log') size 200m;
```

```sql
--删除原有两组日志组，并继续新增两组日志组
alter
system checkpoint;
alter
system switch logfile;
alter
database drop
logfile group 1;
alter
database drop
logfile group 2;
alter
database add logfile group 1 ('/usr/lib/oracle/xe/app/oracle/flash_recovery_area/XE/onlinelog/redo1.log') size 200m;
alter
database add logfile group 2 ('/usr/lib/oracle/xe/app/oracle/flash_recovery_area/XE/onlinelog/redo2.log') size 200m;
```

#### d、查询创建的日志组

```sql
SELECT *
FROM v$log;
SELECT *
FROM v$logfile;
```

![image](/doc/LogMiner/LogMiner7.png)

![image](/doc/LogMiner/LogMiner8.png)

### 5、检查是否安装 LogMiner 工具

Oracle10g 默认已安装 LogMiner 工具包，通过以下命令查询：

```sql
desc DBMS_LOGMNR;
desc DBMS_LOGMNR_D;
```

若无信息打印，则执行下列 SQL 初始化 LogMiner 工具包：

```sql
@
$ORACLE_HOME
/rdbms/admin/dbmslm.sql;
@
$ORACLE_HOME
/rdbms/admin/dbmslmd.sql;
```

### 6、创建 LogMiner 角色并赋权

其中`roma_logminer_privs`为角色名称，可根据自身需求修改。

```sql
create role roma_logminer_privs;
grant
create
session,execute_catalog_role,select any transaction,flashback any table,select any table,lock any table,select any dictionary to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_COL$ to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_OBJ$ to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_USER$ to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_UID$ to roma_logminer_privs;
grant select_catalog_role to roma_logminer_privs;
```

### 7、创建 LogMiner 用户并赋权

其中`roma_logminer`为用户名，`password`为密码，请根据自身需求修改。

```sql
create
user roma_logminer identified by password default tablespace users;
grant roma_logminer_privs to roma_logminer;
grant execute_catalog_role to roma_logminer;
alter
user roma_logminer quota unlimited on users;
```

### 8、验证用户权限

以创建的 LogMiner 用户登录 Oracle 数据库，执行以下 SQL 查询权限，结果如图所示：

```sql
 SELECT *
 FROM USER_ROLE_PRIVS;
```

![image](/doc/LogMiner/LogMiner9.png)

```sql
SELECT *
FROM SESSION_PRIVS;
```

![image](/doc/LogMiner/LogMiner10.png)

至此，Oracle 10g 数据库 LogMiner 实时采集配置完毕。

## 二、Oracle 11g(单机版)

### 1、查询 Oracle 版本信息，这里配置的是`Oracle 11g`

```sql
--查看oracle版本
select *
from v$version;
```

![image](/doc/LogMiner/LogMiner11.png)

本章 Oracle 的版本如上图所示。

### 2、通过命令行方式登录 Oracle，查看是否开启日志归档

```sql
--查询数据库归档模式
archive
log list;
```

![image](/doc/LogMiner/LogMiner12.png)

图中显示`No Archive Mode`表示未开启日志归档。

### 3、开启日志归档，开启日志归档需要重启数据库，请注意

#### a、配置归档日志保存的路径

根据自身环境配置归档日志保存路径，需要提前创建相应目录及赋予相应访问权限

```sql
 alter
system set log_archive_dest_1='location=/data/oracle/archivelog' scope=spfile;
```

#### b、关闭数据库

```sql
shutdown
immediate;
startup
mount;
```

#### c、开启日志归档

```sql
--开启日志归档
alter
database archivelog;
```

#### d、开启扩充日志

```sql
--开启扩充日志
alter
database add supplemental log data (all) columns;
```

#### e、开启数据库

```sql
alter
database open;
```

再次查询数据库归档模式，`Archive Mode`表示已开启归档模式，`Archive destination`表示归档日志储存路径。
![image](/doc/LogMiner/LogMiner13.png)

### 4、检查是否安装 LogMiner 工具

Oracle11g 默认已安装 LogMiner 工具包，通过以下命令查询：

```sql
desc DBMS_LOGMNR;
desc DBMS_LOGMNR_D;
```

若无信息打印，则执行下列 SQL 初始化 LogMiner 工具包：

```sql
@
$ORACLE_HOME
/rdbms/admin/dbmslm.sql;
@
$ORACLE_HOME
/rdbms/admin/dbmslmd.sql;
```

### 5、创建 LogMiner 角色并赋权

其中`roma_logminer_privs`为角色名称，可根据自身需求修改。

```sql
create role roma_logminer_privs;
grant
create
session,execute_catalog_role,select any transaction,flashback any table,select any table,lock any table,select any dictionary to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_COL$ to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_OBJ$ to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_USER$ to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_UID$ to roma_logminer_privs;
grant select_catalog_role to roma_logminer_privs;
```

### 6、创建 LogMiner 用户并赋权

其中`roma_logminer`为用户名，`password`为密码，请根据自身需求修改。

```sql
create
user roma_logminer identified by password default tablespace users;
grant roma_logminer_privs to roma_logminer;
grant execute_catalog_role to roma_logminer;
alter
user roma_logminer quota unlimited on users;
```

### 7、验证用户权限

以创建的 LogMiner 用户登录 Oracle 数据库，执行以下 SQL 查询权限，结果如图所示：

```sql
 SELECT *
 FROM USER_ROLE_PRIVS;
```

![image](/doc/LogMiner/LogMiner14.png)

```sql
SELECT *
FROM SESSION_PRIVS;
```

![image](/doc/LogMiner/LogMiner15.png)

至此，Oracle 11g 数据库 LogMiner 实时采集配置完毕。

## 三、Oracle 12c(单机版非 CBD)

### 1、查询 Oracle 版本信息，这里配置的是`Oracle 12c`

```sql
--查看oracle版本
select BANNER
from v$version;
```

![image](/doc/LogMiner/LogMiner16.png)

本章 Oracle 的版本如上图所示。

### 2、通过命令行方式登录 Oracle，查看是否开启日志归档

```sql
--查询数据库归档模式
archive
log list;
```

![image](/doc/LogMiner/LogMiner17.png)

图中显示`No Archive Mode`表示未开启日志归档。

### 3、开启日志归档，开启日志归档需要重启数据库，请注意

#### a、配置归档日志保存的路径

根据自身环境配置归档日志保存路径，需要提前创建相应目录及赋予相应访问权限

```sql
 alter
system set log_archive_dest_1='location=/data/oracle/archivelog' scope=spfile;
```

#### b、关闭数据库

```sql
shutdown
immediate;
startup
mount;
```

#### c、开启日志归档

```sql
--开启日志归档
alter
database archivelog;
```

#### d、开启扩充日志

```sql
--开启扩充日志
alter
database add supplemental log data (all) columns;
```

#### e、开启数据库

```sql
alter
database open;
```

再次查询数据库归档模式，`Archive Mode`表示已开启归档模式，`Archive destination`表示归档日志储存路径。
![image](/doc/LogMiner/LogMiner18.png)

### 4、创建 LogMiner 角色并赋权

其中`roma_logminer_privs`为角色名称，可根据自身需求修改。

```sql
create role roma_logminer_privs;
grant
create
session,execute_catalog_role,select any transaction,flashback any table,select any table,lock any table,logmining,select any dictionary to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_COL$ to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_OBJ$ to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_USER$ to roma_logminer_privs;
grant select on SYSTEM.LOGMNR_UID$ to roma_logminer_privs;
grant select_catalog_role to roma_logminer_privs;
grant LOGMINING to roma_logminer_privs;
```

### 5、创建 LogMiner 用户并赋权

其中`roma_logminer`为用户名，`password`为密码，请根据自身需求修改。

```sql
create
user roma_logminer identified by password default tablespace users;
grant roma_logminer_privs to roma_logminer;
grant execute_catalog_role to roma_logminer;
alter
user roma_logminer quota unlimited on users;
```

### 6、验证用户权限

以创建的 LogMiner 用户登录 Oracle 数据库，执行以下 SQL 查询权限，结果如图所示：

```sql
 SELECT *
 FROM USER_ROLE_PRIVS;
```

![image](/doc/LogMiner/LogMiner19.png)

```sql
SELECT *
FROM SESSION_PRIVS;
```

![image](/doc/LogMiner/LogMiner20.png)

至此，Oracle 12c 数据库 LogMiner 实时采集配置完毕。
