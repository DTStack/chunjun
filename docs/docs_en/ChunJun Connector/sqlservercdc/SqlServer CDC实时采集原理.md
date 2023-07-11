# SqlServer CDC 实时采集原理

## 一、基础

SqlServer 官方从 SqlServer 2008 版本开始支持 CDC，文档连接如下：
[about-change-data-capture-sql-server](https://docs.microsoft.com/zh-cn/sql/relational-databases/track-changes/about-change-data-capture-sql-server?view=sql-server-ver15)

## 二、配置

配置文档链接如下：
[SqlServer 配置 CDC](SqlServer配置CDC.md)

## 三、原理

### 1、SQL Server Agent

SQL Server Agent 代理服务，是 sql
server 的一个标准服务，作用是代理执行所有 sql 的自动化任务，以及数据库事务性复制等无人值守任务。这个服务在默认安装情况下是停止状态，需要手动启动，或改为自动运动，否则 sql 的自动化任务都不会执行的，还要注意服务的启动帐户。
简单的说就是启动了这个服务，捕获进程才会处理事务日志并将条目写入 CDC 表。
[sql-server-agent](https://docs.microsoft.com/zh-cn/sql/ssms/agent/sql-server-agent?view=sql-server-ver15)

### 2、数据库 CDC 开启前后对比

开启前：

![image](/doc/SqlserverCDC/Sqlserver7.png)

开启后：

EXEC sys.sp_cdc_enable_db;

![image](/doc/SqlserverCDC/Sqlserver8.png)

我们首先观察 dbo 下新增了一张**systranschemas**表，**systranschemas**表用于跟踪事务发布和快照发布中发布的项目中的架构更改。

| 列名称          | 数据类型 | 说明                         |
| --------------- | -------- | ---------------------------- |
| tabid           | int      | 标识发生了架构更改的表项目。 |
| startlsn 时发生 | binary   | 架构更改开始时的 LSN 值。    |
| endlsn          | binary   | 架构更改结束时的 LSN 值。    |
| typeid          | int      | 架构更改的类型。             |

数据库下新增了名为 cdc 的 schema，其实也新增了 cdc 用户。cdc 下新增了以下四张表：
<br/>

- 1、captured_columns
  为在捕获实例中跟踪的每一列返回一行。 默认情况下，将捕获源表中的所有列。 但是，如果为变更数据捕获启用了源表，则可以通过指定列列表将列包括在捕获范围内或排除在捕获范围之外。 当没有任何业务表开启了 CDC 时，该表为空。

| 列名称         | 数据类型 | 说明                                                                                         |
| -------------- | -------- | -------------------------------------------------------------------------------------------- |
| object_id      | int      | 捕获的列所属的更改表的 ID。                                                                  |
| column_name    | sysname  | 捕获的列的名称。                                                                             |
| column_id      | int      | 捕获的列在源表内的 ID。                                                                      |
| column_type    | sysname  | 捕获的列的类型。                                                                             |
| column_ordinal | int      | 更改表中的列序号（从 1 开始）。 将排除更改表中的元数据列。 序号 1 将分配给捕获到的第一个列。 |
| is_computed    | bit      | 表示捕获到的列是源表中计算所得的列。                                                         |

- 2、change_tables
  为数据库中的每个更改表返回一行。 对源表启用变更数据捕获时，将创建一个更改表。 当没有任何业务表开启了 CDC 时，该表为空。

| 列名称               | 数据类型   | 说明                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| -------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| object_id            | int        | 更改表的 ID。 在数据库中是唯一的。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| version              | int        | 标识为仅供参考。 不支持。 不保证以后的兼容性。对于 SQL Server 2012 (11.x)，此列始终返回 0。                                                                                                                                                                                                                                                                                                                                                                                                                        |
| source_object_id     | int        | 为变更数据捕获启用的源表的 ID。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| capture_instance     | sysname    | 用于命名特定于实例的跟踪对象的捕获实例的名称。 默认情况下，该名称从源架构名称加上源表名称派生，格式 schemaname_sourcename。                                                                                                                                                                                                                                                                                                                                                                                        |
| start_lsn            | binary(10) | 日志序列号 (LSN)，表示查询更改表中的更改数据时的低端点。NULL = 尚未建立低端点。                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| end_lsn              | binary(10) | 标识为仅供参考。 不支持。 不保证以后的兼容性。对于 SQL Server 2008，此列始终返回 NULL。                                                                                                                                                                                                                                                                                                                                                                                                                            |
| supports_net_changes | bit        | 对更改表启用了查询净更改支持。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| has_drop_pending     | bit        | 捕获进程收到关于源表已被删除的通知。                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| role_name            | sysname    | 用于访问更改数据的数据库角色的名称。NULL = 未使用角色。                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| index_name           | sysname    | 用于唯一标识源表中的行的索引名称。 index_name 为源表的主键索引的名称，或者在对源表启用了变更数据捕获时指定的唯一索引的名称。NULL = 在变更数据捕获启用时，源表无主键，且未指定唯一索引。注意：如果对具有主键的表启用了变更数据捕获，则不管是否启用了净更改，"变更数据捕获" 功能都将使用索引。 启用变更数据捕获之后，将不允许对主键进行修改。 如果该表没有主键，则仍可以启用变更数据捕获，但是只能将净更改设置为 False。 启用变更数据捕获之后，即可以创建主键。 由于变更数据捕获功能不使用主键，因此还可以修改主键。 |
| filegroup_name       | sysname    | 更改表所驻留的文件组的名称。 NULL = 更改表在数据库的默认文件组中。                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| create_date          | datetime   | 启用源表的日期。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| partition_switch     | bit        | 指示是否可以对启用了变更数据捕获的表执行 ALTER TABLE 的 SWITCH PARTITION 命令。 0 指示分区切换被阻止。 未分区表始终返回 1。                                                                                                                                                                                                                                                                                                                                                                                        |

- 3、ddl_history
  为对启用了变更数据捕获的表所做的每一项数据定义语言 (DDL) 更改返回一行。 可以使用此表来确定源表发生 DDL 更改的时间以及更改的内容。 此表中不包含未发生 DDL 更改的源表的任何条目。
  当没有任何开启了 CDC 的业务表的表结构发生变更时，该表为空。

| 列名称                 | 数据类型      | 说明                                                            |
| ---------------------- | ------------- | --------------------------------------------------------------- |
| source_object_id       | int           | 应用 DDL 更改的源表的 ID。                                      |
| object_id              | int           | 与源表的捕获实例相关联的更改表的 ID。                           |
| required_column_update | bit           | 指示在源表中修改了捕获列的数据类型。 此修改改变了更改表中的列。 |
| ddl_command            | nvarchar(max) | 应用于源表的 DDL 语句。                                         |
| ddl_lsn                | binary(10)    | 与 DDL 修改的提交相关联的日志序列号 (LSN)。                     |
| ddl_time               | datetime      | 对源表所做的 DDL 更改的日期和时间。                             |

- 4、index_columns
  为与更改表关联的每个索引列返回一行。 变更数据捕获使用这些索引列来唯一标识源表中的行。 默认情况下，将包括源表的主键列。 但是，如果在对源表启用变更数据捕获时指定了源表的唯一索引，则将改用该索引中的列。
  如果启用净更改跟踪，则该源表需要主键或唯一索引。 当没有任何开启了 CDC 的业务表存在存在索引列时，该表为空。

| 列名称        | 数据类型 | 说明                          |
| ------------- | -------- | ----------------------------- |
| object_id     | int      | 更改表的 ID。                 |
| column_name   | sysname  | 索引列的名称。                |
| index_ordinal | tinyint  | 索引中的列序号（从 1 开始）。 |
| column_id     | int      | 源表中的列 ID。               |

- 5、lsn_time_mapping
  为每个在更改表中存在行的事务返回一行。 该表用于在日志序列号 (LSN) 提交值和提交事务的时间之间建立映射。 没有对应的更改表项的项也可以记录下来， 以便表在变更活动少或者无变更活动期间将 LSN 处理的完成过程记录下来。

| 列名称          | 数据类型       | 说明                          |
| --------------- | -------------- | ----------------------------- |
| start_lsn       | binary(10)     | 提交的事务的 LSN。            |
| tran_begin_time | datetime       | 与 LSN 关联的事务开始的时间。 |
| tran_end_time   | datetime       | 事务结束的时间。              |
| tran_id         | varbinary (10) | 事务的 ID。                   |

cdc 下新增以下函数：
<br/>

- 1、fn*cdc_get_all_changes*
  为在指定日志序列号 (LSN) 范围内应用到源表的每项更改返回一行。 如果源行在该间隔内有多项更改，则每项更改都会表示在返回的结果集中。 除了返回更改数据外，四个元数据列还提供了将更改应用到另一个数据源所需的信息。
  行筛选选项可控制元数据列的内容以及结果集中返回的行。 当指定“all”行筛选选项时，针对每项更改将只有一行来标识该更改。 当指定“all update
  old”选项时，更新操作会表示为两行：一行包含更新之前已捕获列的值，另一行包含更新之后已捕获列的值。此枚举函数是在对源表启用变更数据捕获时创建的。 函数名称是派生的，并使用 **cdc.fn*cdc_get_all_changes***_
  capture_instance_ 格式，其中 _capture_instance_ 是在源表启用变更数据捕获时为捕获实例指定的值。

| 列名称                            | 数据类型       | 说明                                                                                                                                                                                                                                                                   |
| --------------------------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| \_\_$start_lsn                    | binary(10)     | 与更改关联的提交 LSN，用于保留更改的提交顺序。 在同一事务中提交的更改将共享同一个提交 LSN 值。                                                                                                                                                                         |
| \_\_$seqval                       | binary(10)     | 用于对某事务内的行更改进行排序的序列值。                                                                                                                                                                                                                               |
| \_\_$operation                    | int            | 标识将更改数据行应用到目标数据源所需的数据操作语言 (DML) 操作。 可以是以下值之一：<br/>1 = 删除 <br/> 2 = 插入 <br/> 3 = 更新（捕获的列值是执行更新操作前的值）。 仅当指定了行筛选选项“all update old”时才应用此值。<br/> 4 = 更新（捕获的列值是执行更新操作后的值）。 |
| \_\_$update_mask                  | varbinary(128) | 位掩码，为捕获实例标识的每个已捕获列均对应于一个位。 当 ** $ operation = 1 或 2 时，该值将所有已定义的位设置为 1。 当 ** $ operation = 3 或 4 时，只有与更改的列相对应的位设置为 1。                                                                                   |
| \<captured source table columns\> | 多种多样       | 函数返回的其余列是在创建捕获实例时标识的已捕获列。 如果已捕获列的列表中未指定任何列，则将返回源表中的所有列。                                                                                                                                                          |

- 2、fn*cdc_get_net_changes*
  为 (LSN) 范围内的指定日志序列号内的每个源行返回一个净更改行，返回格式跟上面一样。

### 3、业务表 CDC 开启前后对比

开启前跟上一张图一致

开启 SQL：

```sql
sys
.
sp_cdc_enable_table
-- 表所属的架构名
[ @source_schema = ] 'source_schema',

-- 表名
[ @source_name = ] 'source_name' ,

-- 是用于控制更改数据访问的数据库角色的名称
[ @role_name = ] 'role_name'

-- 是用于命名变更数据捕获对象的捕获实例的名称，这个名称在后面的存储过程和函数中需要经常用到。
[,[ @capture_instance = ] 'capture_instance' ]

-- 指示是否对此捕获实例启用净更改查询支持如果此表有主键，或者有已使用 @index_name 参数进行标识的唯一索引，则此参数的默认值为 1。否则，此参数默认为 0。
[,[ @supports_net_changes = ] supports_net_changes ]

-- 用于唯一标识源表中的行的唯一索引的名称。index_name 为 sysname，并且可以为 NULL。
-- 如果指定，则 index_name 必须是源表的唯一有效索引。如果指定 index_name，则标识的索引列优先于任何定义的主键列，就像表的唯一行标识符一样。
[,[ @index_name = ] 'index_name' ]

-- 需要对哪些列进行捕获。captured_column_list 的数据类型为 nvarchar(max)，并且可以为 NULL。如果为 NULL，则所有列都将包括在更改表中。
[,[ @captured_column_list = ] 'captured_column_list' ]

-- 是要用于为捕获实例创建的更改表的文件组。
[,[ @filegroup_name = ] 'filegroup_name' ]

-- 指示是否可以对启用了变更数据捕获的表执行 ALTER TABLE 的 SWITCH PARTITION 命令。
-- allow_partition_switch 为 bit，默认值为 1。
[,[ @partition_switch = ] 'partition_switch' ]
```

开启后：

![image](/doc/SqlserverCDC/Sqlserver9.png)

此时，cdc 下新增了一张名为 dbo*kudu_CT 的表，对于任意开启 CDC 的业务表而言，都会在其对应的 cdc schema 下创建一张格式为$schema*$table}\_CT 的表。

**1、dbo_kudu_CT：**
对源表启用变更数据捕获时创建的更改表。 该表为对源表执行的每个插入和删除操作返回一行，为对源表执行的每个更新操作返回两行。 如果在启用源表时未指定更改表的名称，则会使用一个派生的名称。 名称的格式为
cdc。capture_instance \_CT 其中 capture_instance 是源表的架构名称和格式 schema_table 的源表名称。 例如，如果对 AdventureWorks 示例数据库中的表 Person
启用了变更数据捕获，则派生的更改表名称将 cdc.Person_Address_CT。

| 列名称                            | 数据类型       | 说明                                                                                                                                                                                                         |
| --------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| \_\_$start_lsn                    | binary(10)     | 与相应更改的提交事务关联的日志序列号 (LSN)。在同一事务中提交的所有更改将共享同一个提交 LSN。 例如，如果对源表的 delete 操作删除两行，则更改表将包含两行，每行都具有相同的 \_\_ $ start_lsn 值。              |
| \_\_ $ end_lsn                    | binary(10)     | 标识为仅供参考。 不支持。 不保证以后的兼容性。在 SQL Server 2012 (11.x) 中，此列始终为 NULL。                                                                                                                |
| \_\_$seqval                       | binary(10)     | 用于对事务内的行更改进行排序的序列值。                                                                                                                                                                       |
| \_\_$operation                    | int            | 标识与相应更改关联的数据操作语言 (DML) 操作。 可以是以下值之一：<br/>1 = 删除<br/>2 = 插入<br/>3 = 更新（旧值）列数据中具有执行更新语句之前的行值。<br/>4 = 更新（新值）列数据中具有执行更新语句之后的行值。 |
| \_\_$update_mask                  | varbinary(128) | 基于更改表的列序号的位掩码，用于标识那些发生更改的列。                                                                                                                                                       |
| \<captured source table columns\> | 多种多样       | 更改表中的其余列是在创建捕获实例时源表中标识为已捕获列的那些列。 如果已捕获列的列表中未指定任何列，则源表中的所有列将包括在此表中。                                                                          |
| \_\_ $ command_id                 | int            | 跟踪事务中的操作顺序。                                                                                                                                                                                       |

**2、captured_columns：**

![image](/doc/SqlserverCDC/Sqlserver10.png)

**3、change_tables：**

![image](/doc/SqlserverCDC/Sqlserver11.png)

### 4、采集原理

#### 1、insert/delete

对于 insert 和 delete 类型的数据变更，对于每一行变更都会在对应的${schema}_${table}\_
CT 表中增加一行记录。对于 insert，id，user_id，name 记录的是 insert 之后的 value 值；对于 delete，id，user_id，name 记录的是 delete 之前的 value 值；
![image](/doc/SqlserverCDC/Sqlserver12.png)

#### 2、update

a、更新了主键 此时，SqlServer 数据库的做法是在同一事物内，先将原来的记录删除，然后再重新插入。 执行如下 SQL，日志表如图所示： UPDATE [dbo].[kudu] SET [id] = 2, [user_id] = '
2', [name] = 'b' WHERE [id] = 1;
![image](/doc/SqlserverCDC/Sqlserver13.png)

b、未更新主键
此时，SqlServer 数据库的做法是直接更新字段信息。
执行如下 SQL，日志表如图所示：
UPDATE [dbo].[kudu] SET [user_id] = '3', [name] = 'c' WHERE [id] = 2;

![image](/doc/SqlserverCDC/Sqlserver14.png)

#### 3、流程图

![image](/doc/SqlserverCDC/SqlserverCdc流程图.png)

对于 ChunJun SqlServer CDC 实时采集插件，其基本原理便是以轮询的方式，循环调用 fn*cdc_get_all_changes*函数，获取上次结束时的 lsn 与当前数据库最大 lsn 值之间的数据。对于 insert/delete 类型的数据获取并解析一行，对于 update 类型获取并解析两行。解析完成后把数据传递到下游并记录当前解析到的数据的 lsn，为下次轮询做准备。

#### 4、数据格式

```json
{
  "type": "update",
  "schema": "dbo",
  "table": "tb1",
  "lsn": "00000032:00002038:0005",
  "ts": 6760525407742726144,
  "before_id": 1,
  "after_id": 2
}
```
