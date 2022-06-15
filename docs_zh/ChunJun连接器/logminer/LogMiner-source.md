# LogMiner Source

## 一、介绍

OracleLogMiner插件支持配置监听表名称以及读取起点读取日志数据。OracleLogMiner在checkpoint时保存当前消费的位点，因此支持续跑。

## 二、支持版本

oracle10,oracle11,oracle12,oracle19，支持RAC,主备架构

## 三、数据库配置

[Oracle配置LogMiner](LogMiner配置.md)

## 四、LogMiner原理

[LogMiner原理](LogMiner原理.md)

## 五、插件名称

| Sync | oraclelogminerreader、oraclelogminersource |
| --- | --- |
| SQL | oraclelogminer-x |

##      

## 六、参数说明

### 1、Sync

- **jdbcUrl**
    - 描述：oracle数据库的JDBC URL链接
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
    - 描述： 需要监听的表，格式为：schema.table，schema不能配置为*，但table可以配置*监听指定库下所有的表，如：["schema1.table1","schema1.table2","schema2.*"]
    - 必选：否，不配置则监听除`SYS`库以外的所有库的所有表变更信息
    - 字段类型：数组
    - 默认值：无


- **splitUpdate**
    - 描述：当数据更新类型为update时，是否将update拆分为两条数据，具体见【七、数据结构说明】
    - 必选：否
    - 字段类型：boolean
    - 默认值：false


- **cat**
    - 描述：需要监听的操作数据操作类型，有UPDATE,INSERT,DELETE三种可选，大小写不敏感，多个以,分割
    - 必选：否
    - 字段类型：String
    - 默认值：UPDATE,INSERT,DELETE


- **readPosition**
    - 描述：Oracle实时采集的采集起点
    - 可选值：
        - all： 从Oracle数据库中最早的归档日志组开始采集(不建议使用)
        - current：从任务运行时开始采集
        - time： 从指定时间点开始采集
        - scn： 从指定SCN号处开始采集
    - 必选：否
    - 字段类型：String
    - 默认值：current


- **startTime**
    - 描述： 指定采集起点的毫秒级时间戳
    - 必选：当`readPosition`为`time`时，该参数必填
    - 字段类型：Long(毫秒级时间戳)
    - 默认值：无


- **startSCN**
    - 描述： 指定采集起点的SCN号
    - 必选：当`readPosition`为`scn`时，该参数必填
    - 字段类型：String
    - 默认值：无


- **fetchSize**
    - 描述： 批量从v$logmnr_contents视图中拉取的数据条数，对于大数据量的数据变更，调大该值可一定程度上增加任务的读取速度
    - 必选：否
    - 字段类型：Integer
    - 默认值：1000


- **queryTimeout**
    - 描述： LogMiner执行查询SQL的超时参数，单位秒
    - 必选：否
    - 字段类型：Long
    - 默认值：300


- **supportAutoAddLog**
    - 描述：启动LogMiner是否自动添加日志组(不建议使用)
    - 必选：否
    - 字段类型：Boolean
    - 默认值：false


- **pavingData**
    - 描述：是否将解析出的json数据拍平，具体见【七、数据结构说明】
    - 必选：否
    - 字段类型：boolean
    - 默认值：false

### 2、SQL

- **url**
    - 描述：oracle数据库的JDBC URL链接
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


- **read-position**
    - 描述：Oracle实时采集的采集起点
    - 可选值：
        - all： 从Oracle数据库中最早的归档日志组开始采集(不建议使用)
        - current：从任务运行时开始采集
        - time： 从指定时间点开始采集
        - scn： 从指定SCN号处开始采集
    - 必选：否
    - 字段类型：String
    - 默认值：current


- **start-time**
    - 描述： 指定采集起点的毫秒级时间戳
    - 必选：当`readPosition`为`time`时，该参数必填
    - 字段类型：Long(毫秒级时间戳)
    - 默认值：无


- **start-scn**
    - 描述： 指定采集起点的SCN号
    - 必选：当`readPosition`为`scn`时，该参数必填
    - 字段类型：String
    - 默认值：无


- **fetch-size**
    - 描述： 批量从v$logmnr_contents视图中拉取的数据条数，对于大数据量的数据变更，调大该值可一定程度上增加任务的读取速度
    - 必选：否
    - 字段类型：Integer
    - 默认值：1000


- **query-timeout**
    - 描述： LogMiner执行查询SQL的超时参数，单位秒
    - 必选：否
    - 字段类型：Long
    - 默认值：300


- **support-auto-add-log**
    - 描述：启动LogMiner是否自动添加日志组(不建议使用)
    - 必选：否
    - 字段类型：Boolean
    - 默认值：false


- **io-threads**
    - 描述：IO处理线程数,最大线程数为3
    - 必选：否
    - 字段类型：int
    - 默认值：1


- **max-log-file-size**
    - 描述：logminer一次性加载的日志文件的大小，默认5g，单位byte
    - 必选：否
    - 字段类型：long
    - 默认值：5*1024*1024*1024


- **transaction-cache-num-size**
    - 描述：logminer可缓存DML的数量
    - 必选：否
    - 字段类型：long
    - 默认值：800


- **transaction-expire-time**
    - 描述：logminer可缓存的失效时间，单位分钟
    - 必选：否
    - 字段类型：int
    - 默认值：20

## 七、数据结构

在2021-06-29 23:42:19(时间戳：1624981339000)执行：

```sql
INSERT INTO TIEZHU.RESULT1 ("id", "name", "age")
VALUES (1, 'a', 12)
```

在2021-06-29 23:42:29(时间戳：1624981349000)执行：

```sql
UPDATE TIEZHU.RESULT1 t
SET t."id"  = 2,
    t."age" = 112
WHERE t."id" = 1
```

在2021-06-29 23:42:34(时间戳：1624981354000)执行：

```sql
 DELETE
 FROM TIEZHU.RESULT1
 WHERE "id" = 2 
```

1、pavingData = true, splitUpdate = false RowData中的数据依次为：

```
//scn schema, table, ts, opTime, type, before_id, before_name, before_age, after_id, after_name, after_age
[49982945,"TIEZHU", "RESULT1", 6815665753853923328, "2021-06-29 23:42:19.0", "INSERT", null, null, null, 1, "a", 12]
[49982969,"TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE", 1, "a", 12 , 2, "a", 112]
[49982973,"TIEZHU", "RESULT1", 6815665796140896256, "2021-06-29 23:42:34.0", "DELETE", 2, "a",112 , null, null, null]
```

2、pavingData = false, splitUpdate = false RowData中的数据依次为：

```
//scn, schema, table,  ts, opTime, type, before, after
[49982945, "TIEZHU", "RESULT1", 6815665753853923328, "2021-06-29 23:42:19.0", "INSERT", null, {"id":1, "name":"a", "age":12}]
[49982969, "TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE", {"id":1, "name":"a", "age":12}, {"id":2, "name":"a", "age":112}]
[49982973, "TIEZHU", "RESULT1", 6815665796140896256, "2021-06-29 23:42:34.0", "DELETE", {"id":2, "name":"a", "age":112}, null]
```

3、pavingData = true, splitUpdate = true RowData中的数据依次为：

```
//scn, schema, table, ts, opTime, type, before_id, before_name, before_age, after_id, after_name, after_age
[49982945,"TIEZHU", "RESULT1", 6815665753853923328, "2021-06-29 23:42:19.0", "INSERT", null, null, null, 1, "a",12 ]

//scn, schema, table, opTime, ts, type, before_id, before_name, before_age
[49982969, "TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE_BEFORE", 1, "a", 12]
//scn, schema, table, opTime, ts, type, after_id, after_name, after_age
[49982969, "TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE_AFTER", 2, "a", 112]

//scn, schema, table, ts, opTime, type, before_id, before_name, before_age, after_id, after_name, after_age
[49982973, "TIEZHU", "RESULT1", 6815665796140896256,  "2021-06-29 23:42:34.0", "DELETE", 2, "a", 112, null, null, null]


```

4、pavingData = false, splitUpdate = true RowData中的数据依次为：

```
//scn, schema, table,  ts, opTime, type, before, after
[49982945, "TIEZHU", "RESULT1", 6815665753853923328, "2021-06-29 23:42:19.0", "INSERT", null, {"id":1, "name":"a", "age":12}]
//scn, schema, table,  ts, opTime, type, before
[49982969, "TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE_BEFORE", {"id":1, "name":"a", "age":12}]
//scn, schema, table,  ts, opTime, type, after
[49982969, "TIEZHU", "RESULT1", 6815665796098953216, "2021-06-29 23:42:29.0", "UPDATE_AFTER", {"id":2, "name":"a", "age":112}]
//scn, schema, table, ts, opTime, type, before, after
[49982973, "TIEZHU", "RESULT1", 6815665796140896256, "2021-06-29 23:42:34.0",  "DELETE", {"id":2, "name":"a", "age":112}, null]

```

- scn：Oracle数据库变更记录对应的scn号
- type：变更类型，INSERT，UPDATE、DELETE
- opTime：数据库中SQL的执行时间
- ts：自增ID，不重复，可用于排序，解码后为ChunJun的事件时间，解码规则如下:

```java
long id=Long.parseLong("6815665753853923328");
        long res=id>>22;
        DateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sdf.format(res));        //2021-06-29 23:42:24
```

## 八、数据类型

| 是否支持 | 数据类型 |
| --- | --- |
| 支持 | DATE,TIMESTAMP,TIMESTAMP WITH LOCAL TIME ZONE,TIMESTAMP WITH TIME ZONE, CHAR,NCHAR,NVARCHAR2,ROWID,VARCHAR2,VARCHAR,LONG,RAW,LONG RAW,INTERVAL YEAR,INTERVAL DAY,BLOB,CLOB,NCLOB, NUMBER,SMALLINT,INT INTEGER,FLOAT,DECIMAL,NUMERIC,BINARY_FLOAT,BINARY_DOUBLE  |
| 不支持 | BFILE,XMLTYPE,Collections |

## 九、脚本示例

见项目内`ChunJun : Local : Test`模块中的`demo`文件夹。
