# SqlserverCDC Source

## 一、介绍

Sqlservercdc插件支持配置监听表名称以及读取起点读取日志数据。SQLservercdc在checkpoint时保存当前消费的lsn，因此支持续跑。

## 二、支持版本

SqlServer 2012、2014、2016、2017、2019单机版

## 三、数据库配置

[SqlserverCDC配置](SqlServer配置CDC.md)

## 四、SqlserverCDC原理

[SqlserverCDC原理](SqlServer CDC实时采集原理.md)

## 五、插件名称

| Sync | sqlservercdcreader、sqlservercdcsource |
| --- | --- |
| SQL | sqlservercdc-x |

##

## 六、参数说明

### 1、Sync

- **url**
    - 描述：sqlserver数据库的JDBC URL链接
    - 必选：是
    - 参数类型：string
    - 默认值：无


- **username**
    - 描述：用户名
    - 必选：是
    - 参数类型：string
    - 默认值：无


- **password**
    - 描述：密码
    - 必选：是
    - 参数类型：string
    - 默认值：无


- **tableList**
    - 描述： 需要监听的表，如：["schema1.table1","schema1.table2"]
    - 必选：是
    - 字段类型：数组
    - 默认值：无


- **splitUpdate**
    - 描述：当数据更新类型为update时，是否将update拆分为两条数据，具体见【六、数据结构说明】
    - 必选：否
    - 字段类型：boolean
    - 默认值：false


- **cat**
    - 描述：需要监听的操作数据操作类型，有UPDATE,INSERT,DELETE三种可选，大小写不敏感，多个以,分割
    - 必选：否
    - 字段类型：String
    - 默认值：UPDATE,INSERT,DELETE


- **lsn**
    - 描述： 要读取SqlServer CDC日志序列号的开始位置
    - 必选： 否
    - 字段类型：String(00000032:00002038:0005)
    - 默认值：无


- **pollInterval**
    - 描述： 监听拉取SqlServer CDC数据库间隔时间,该值越小，采集延迟时间越小，给数据库的访问压力越大
    - 必选：否
    - 字段类型：long(单位毫秒)
    - 默认值：1000


- **pavingData**
    - 描述：是否将解析出的json数据拍平，具体见【七、数据结构说明】
    - 必选：否
    - 字段类型：boolean
    - 默认值：false

### 2、SQL

- **url**
    - 描述：sqlserver数据库的JDBC URL链接
    - 必选：是
    - 参数类型：string
    - 默认值：无


- **username**
    - 描述：用户名
    - 必选：是
    - 参数类型：string
    - 默认值：无


- **password**
    - 描述：密码
    - 必选：是
    - 参数类型：string
    - 默认值：无


- **table**
    - 描述：需要解析的数据表。
    - 注意：SQL任务只支持监听单张表，且数据格式为schema.table
    - 必选：否
    - 字段类型：string
    - 默认值：无


- **cat**
    - 描述：需要监听的操作数据操作类型，有UPDATE,INSERT,DELETE三种可选，大小写不敏感，多个以,分割
    - 必选：否
    - 字段类型：String
    - 默认值：UPDATE,INSERT,DELETE


- **lsn**
    - 描述： 要读取SqlServer CDC日志序列号的开始位置
    - 必选： 否
    - 字段类型：String(00000032:00002038:0005)
    - 默认值：无


- **poll-interval**
    - 描述： 监听拉取SqlServer CDC数据库间隔时间,该值越小，采集延迟时间越小，给数据库的访问压力越大
    - 必选：否
    - 字段类型：long(单位毫秒)
    - 默认值：1000

## 七、数据结构

在2020-01-01 12:30:00(时间戳：1577853000000)执行：

```sql
INSERT INTO `tudou`.`kudu`(`id`, `user_id`, `name`)
VALUES (1, 1, 'a');
```

在2020-01-01 12:31:00(时间戳：1577853060000)执行：

```sql
DELETE
FROM `tudou`.`kudu`
WHERE `id` = 1
  AND `user_id` = 1
  AND `name` = 'a';
```

在2020-01-01 12:32:00(时间戳：1577853180000)执行：

```sql
UPDATE `tudou`.`kudu`
SET `id`      = 2,
    `user_id` = 2,
    `name`    = 'b'
WHERE `id` = 1
  AND `user_id` = 1
  AND `name` = 'a';
```

1、pavingData = true, splitUpdate = false RowData中的数据依次为：

```
//schema, table, ts, opTime, type, before_id, before_user_id, before_name, after_id, after_user_id, after_name
["tudou", "kudu", 6760525407742726144, 1577853000000, "INSERT", null, null, null, 1, 1, "a"]
["tudou", "kudu", 6760525407742726144, 1577853060000, "DELETE", 1, 1, "a", null, null, null]
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE", 1, 1, "a", 2, 2, "b"]
```

2、pavingData = false, splitUpdate = false RowData中的数据依次为：

```
//schema, table, ts, opTime, type, before, after
["tudou", "kudu", 6760525407742726144, 1577853000000, "INSERT", null, {"id":1, "user_id":1, "name":"a"}]
["tudou", "kudu", 6760525407742726144, 1577853060000, "DELETE", {"id":1, "user_id":1, "name":"a"}, null]
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE", {"id":1, "user_id":1, "name":"a"}, {"id":2, "user_id":2, "name":"b"}]
```

3、pavingData = true, splitUpdate = true RowData中的数据依次为：

```
//schema, table, ts, opTime, type, before_id, before_user_id, before_name, after_id, after_user_id, after_name
["tudou", "kudu", 6760525407742726144, 1577853000000, "INSERT", null, null, null, 1, 1, "a"]
["tudou", "kudu", 6760525407742726144, 1577853060000, "DELETE", 1, 1, "a", null, null, null]

//schema, table, ts, opTime, type, before_id, before_user_id, before_name
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE_BEFORE", 1, 1, "a"]

//schema, table, ts, opTime, type, after_id, after_user_id, after_name
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE_AFTER", 2, 2, "b"]
```

4、pavingData = false, splitUpdate = true RowData中的数据依次为：

```
//schema, table, ts, opTime, type, before, after
["tudou", "kudu", 6760525407742726144, 1577853000000, "INSERT", null, {"id":1, "user_id":1, "name":"a"}]
["tudou", "kudu", 6760525407742726144, 1577853060000, "DELETE", {"id":1, "user_id":1, "name":"a"}, null]
//schema, table, ts, opTime, type, before
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE_BEFORE", {"id":1, "user_id":1, "name":"a"}]
//schema, table, ts, opTime, type, after
["tudou", "kudu", 6760525407742726144, 1577853180000, "UPDATE_AFTER", {"id":2, "user_id":2, "name":"b"}]
```

- type：变更类型，INSERT，UPDATE、DELETE
- opTime：数据库中SQL的执行时间
- ts：自增ID，不重复，可用于排序，解码后为ChunJun的事件时间，解码规则如下:

```java
long id=Long.parseLong("6760525407742726144");
        long res=id>>22;
        DateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sdf.format(res));        //2021-01-28 19:54:21
```

## 八、数据类型

| 是否支持 | 数据类型 |
| --- | --- |
| 支持 | BIT, TINYINT24, INT, INTEGER, FLOAT, DOUBLE, REAL, LONG, BIGINT, DECIMAL, NUMERIC, BINARY, VARBINARY, DATE, TIME, TIMESTAMP, DATETIME, DATETIME2, SMALLDATETIME, CHAR, VARCHAR, NCHAR, NVARCHAR, TEXT  |
| 不支持 | ROWVERSION, UNIQUEIDENTIFIER, CURSOR, TABLE, SQL_VARIANT |

## 九、脚本示例

见项目内`chunjun-examples`文件夹。
