# Binlog Source

<!-- TOC -->

## 一、介绍

Binlog插件使用Canal组件实时地从MySQL中捕获变更数据。source同步插件支持数据还原，支持DDL的变更。

## 二、支持版本

MySQL 5.1.5及以上、TiDB 3.0.10之后

## 三、插件名称

| Sync | binlogsource、binlogreader |
| --- | --- |
| SQL | binlog-x |

## 四、数据库配置

### 1、修改配置文件

binlog_format需要修改为 ROW 格式，在/etc/my.cnf文件里[mysqld]下添加下列配置

```sql
server_id
=109
log_bin = /var/lib/mysql/mysql-bin
binlog_format = ROW
expire_logs_days = 30
```

### 2、添加权限

MySQL Binlog权限需要三个权限 SELECT, REPLICATION SLAVE, REPLICATION CLIENT

```sql
GRANT
SELECT, REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'canal'@'%' IDENTIFIED BY 'canal';
```

- 缺乏SELECT权限时，报错为

```text
com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException:
Access denied for user 'canal'@'%' to database 'binlog'
```

- 缺乏REPLICATION SLAVE权限时，报错为

```text
java.io.IOException: 
Error When doing Register slave:ErrorPacket [errorNumber=1045, fieldCount=-1, message=Access denied for user 'canal'@'%'
```

- 缺乏REPLICATION CLIENT权限时，报错为

```text
com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException: 
Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation
```

Binlog为什么需要这些权限：

- Select权限代表允许从表中查看数据
- Replication client权限代表允许执行show master status,show slave status,show binary logs命令
- Replication slave权限代表允许slave主机通过此用户连接master以便建立主从 复制关系

## 五、参数说明

### 1、Sync

- **jdbcUrl**
  -
  描述：MySQL数据库的jdbc连接字符串，参考文档：[Mysql官方文档](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)
    - 必选：是
    - 字段类型：string
    - 默认值：无


- **username**
    - 描述：数据源的用户名
    - 必选：是
    - 字段类型：string
    - 默认值：无


- **password**
    - 描述：数据源指定用户名的密码
    - 必选：是
    - 字段类型：string
    - 默认值：无


- **host**
    - 描述：启动MySQL slave的机器ip
    - 必选：是
    - 字段类型：string
    - 默认值：无


- **port**
    - 描述：启动MySQL slave的端口
    - 必选：否
    - 字段类型：int
    - 默认值：3306


- **table**
    - 描述：需要解析的数据表。
    - 注意：指定此参数后filter参数将无效,table和filter都为空，监听jdbcUrl里的schema下所有表
    - 必选：否
    - 字段类型：list\<string\>
    - 默认值：无


- **filter**
    - 描述：过滤表名的Perl正则表达式
    - 注意：table和filter都为空，监听jdbcUrl里的schema下所有表
    - 必选：否
    - 字段类型：string
    - 默认值：无
    - 例子：
        - 所有表：`.*` or `.*\\..*`
        - canal schema下所有表： `canal\\..*`
        - canal下的以canal打头的表：`canal\\.canal.*`
        - canal schema下的一张表：`canal.test1`


- **cat**
    - 描述：需要解析的数据更新类型，包括insert、update、delete三种
    - 注意：以英文逗号分割的格式填写。如果为空，解析所有数据更新类型
    - 必选：否
    - 字段类型：string
    - 默认值：无


- **start**
    - 描述：要读取的binlog文件的开始位置
    - 注意：为空，则从当前position处消费，timestamp的优先级高于 journal-name+position
    - 参数：
        - timestamp：时间戳，采集起点从指定的时间戳处消费；
        - journal-name：文件名，采集起点从指定文件的起始处消费；
        - position：文件的指定位置，采集起点从指定文件的指定位置处消费
    - 字段类型：map
    - 默认值：无


- **pavingData**
    - 描述：是否将解析出的json数据拍平，具体见[六、数据结构](#六数据结构)
    - 必选：否
    - 字段类型：boolean
    - 默认值：true


- **splitUpdate**
    - 描述：当数据更新类型为update时，是否将update拆分为两条数据，具体见[六、数据结构](#六数据结构)
    - 必选：否
    - 字段类型：boolean
    - 默认值：false


- **timestampFormat**
    - 描述：指定输入输出所使用的timestamp格式，可选值：`SQL`、`ISO_8601`
    - 必选：否
    - 字段类型：string
    - 默认值：SQL


- **slaveId**
    - 描述：从服务器的ID
    - 注意：同一个MYSQL复制组内不能重复
    - 必选：否
    - 字段类型：long
    - 默认值：new Object().hashCode()


- **connectionCharset**
    - 描述：编码信息
    - 必选：否
    - 字段类型：string
    - 默认值：UTF-8


- **detectingEnable**
    - 描述：是否开启心跳
    - 必选：否
    - 字段类型：boolean
    - 默认值：true


- **detectingSQL**
    - 描述：心跳SQL
    - 必选：否
    - 字段类型：string
    - 默认值：SELECT CURRENT_DATE


- **enableTsdb**
    - 描述：是否开启时序表结构能力
    - 必选：否
    - 字段类型：boolean
    - 默认值：true


- **bufferSize**
    - 描述：并发缓存大小
    - 注意：必须为2的幂
    - 必选：否
    - 默认值：1024


- **parallel**
    - 描述：是否开启并行解析binlog日志
    - 必选：否
    - 字段类型：boolean
    - 默认值：true


- **parallelThreadSize**
    - 描述：并行解析binlog日志线程数
    - 注意：只有 paraller 设置为true才生效
    - 必选：否
    - 字段类型：int
    - 默认值：2


- **isGTIDMode**
    - 描述：是否开启gtid模式
    - 必选：否
    - 字段类型：boolean
    - 默认值：false


- **queryTimeOut**
    - 描述：通过TCP连接发送数据(在这里就是要执行的sql)后，等待响应的超时时间，单位毫秒
    - 必选：否
    - 字段类型：int
    - 默认值：300000


- **connectTimeOut**
    - 描述：数据库驱动(mysql-connector-java)与mysql服务器建立TCP连接的超时时间，单位毫秒
    - 必选：否
    - 字段类型：int
    - 默认值：60000

### 2、SQL

- **url**
    - 描述：MySQL数据库的jdbc连接字符串，参考文档：[Mysql官方文档](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)
    - 必选：是
    - 字段类型：string
    - 默认值：无


- **username**
    - 描述：数据源的用户名
    - 必选：是
    - 字段类型：string
    - 默认值：无


- **password**
    - 描述：数据源指定用户名的密码
    - 必选：是
    - 字段类型：string
    - 默认值：无


- **host**
    - 描述：启动MySQL slave的机器ip
    - 必选：是
    - 字段类型：string
    - 默认值：无


- **port**
    - 描述：启动MySQL slave的端口
    - 必选：否
    - 字段类型：int
    - 默认值：3306


- **table**
    - 描述：需要解析的数据表。
    - 注意：指定此参数后filter参数将无效，SQL任务只支持监听单张表
    - 必选：否
    - 字段类型：string
    - 默认值：无


- **filter**
    - 描述：过滤表名的Perl正则表达式
    - 注意：SQL任务只支持监听单张表
    - 必选：否
    - 字段类型：string
    - 默认值：无
    - 例子：canal schema下的一张表：`canal.test1`


- **cat**
    - 描述：需要解析的数据更新类型，包括insert、update、delete三种
    - 注意：以英文逗号分割的格式填写。如果为空，解析所有数据更新类型
    - 必选：否
    - 字段类型：string
    - 默认值：无


- **timestamp**
    - 描述：要读取的binlog文件的开始位置，时间戳，采集起点从指定的时间戳处消费；
    - 必选：否
    - 字段类型：string
    - 默认值：无


- **journal-name**
    - 描述：要读取的binlog文件的开始位置，文件名，采集起点从指定文件的起始处消费；
    - 必选：否
    - 字段类型：string
    - 默认值：无


- **position**
    - 描述：要读取的binlog文件的开始位置，文件的指定位置，采集起点从指定文件的指定位置处消费
    - 必选：否
    - 字段类型：string
    - 默认值：无


- **connection-charset**
    - 描述：编码信息
    - 必选：否
    - 字段类型：string
    - 默认值：UTF-8


- **detecting-enable**
    - 描述：是否开启心跳
    - 必选：否
    - 字段类型：boolean
    - 默认值：true


- **detecting-sql**
    - 描述：心跳SQL
    - 必选：否
    - 字段类型：string
    - 默认值：SELECT CURRENT_DATE


- **enable-tsdb**
    - 描述：是否开启时序表结构能力
    - 必选：否
    - 字段类型：boolean
    - 默认值：true


- **buffer-size**
    - 描述：并发缓存大小
    - 注意：必须为2的幂
    - 必选：否
    - 默认值：1024


- **parallel**
    - 描述：是否开启并行解析binlog日志
    - 必选：否
    - 字段类型：boolean
    - 默认值：true


- **parallel-thread-size**
    - 描述：并行解析binlog日志线程数
    - 注意：只有 paraller 设置为true才生效
    - 必选：否
    - 字段类型：int
    - 默认值：2


- **is-gtid-mode**
    - 描述：是否开启gtid模式
    - 必选：否
    - 字段类型：boolean
    - 默认值：false


- **query-time-out**
    - 描述：通过TCP连接发送数据(在这里就是要执行的sql)后，等待响应的超时时间，单位毫秒
    - 必选：否
    - 字段类型：int
    - 默认值：300000


- **connect-time-out**
    - 描述：数据库驱动(mysql-connector-java)与mysql服务器建立TCP连接的超时时间，单位毫秒
    - 必选：否
    - 字段类型：int
    - 默认值：60000


- **timestamp-format.standard**
    - 描述：同Sync中的`timestampFormat`参数，指定输入输出所使用的timestamp格式，可选值：`SQL`、`ISO_8601`
    - 必选：否
    - 字段类型：string
    - 默认值：SQL

## 六、数据结构

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

## 七、数据类型

| 支持 | BIT |
| --- | --- |
|  | TINYINT、SMALLINT、MEDIUMINT、INT、INT24、INTEGER、FLOAT、DOUBLE、REAL、LONG、BIGINT、DECIMAL、NUMERIC |
|  | CHAR、VARCHAR、TINYTEXT、TEXT、MEDIUMTEXT、LONGTEXT、ENUM、SET、JSON |
|  | DATE、TIME、TIMESTAMP、DATETIME、YEAR |
|  | TINYBLOB、BLOB、MEDIUMBLOB、LONGBLOB、GEOMETRY、BINARY、VARBINARY |
| 暂不支持 | 无 |

## 八、脚本示例

见项目内`chunjun-examples`文件夹。
