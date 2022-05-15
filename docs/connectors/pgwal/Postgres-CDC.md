# MySQL Postgres CDC Source


<!-- TOC -->

- [一、介绍](#一介绍)
- [二、支持版本](#二支持版本)
- [三、插件名称](#三插件名称)
- [四、数据库配置](#四数据库配置)
    - [1 、修改配置文件](#1修改配置文件)
    - [2 、添加权限](#2添加权限)
- [五、参数说明](#五参数说明)
    - [1 、Sync](#1sync)
    - [2 、SQL](#2sql)
- [六、数据结构](#六数据结构)
- [七、数据类型](#七数据类型)
- [八、脚本示例](#八脚本示例)

<!-- /TOC -->

##  一、介绍
Postgres CDC 插件实时地从Postgres中捕获变更数据。目前sink插件暂不支持数据还原，只能写入变更的日志数据。

##  二、支持版本
Postgres 10.0以上

## 三、插件名称
| Sync | pgwalsource、pgwalreader |
| --- | --- |
| SQL | pgwal-x |

##  四、数据库配置
###  1、修改配置文件
pgwal_format需要修改为 ROW 格式，在/etc/my.cnf文件里[mysqld]下添加下列配置
```sql
server_id=109
log_bin = /var/lib/mysql/mysql-bin
binlog_format = ROW
expire_logs_days = 30
```


##  五、参数说明
###  1、Sync

- **url**
    - 描述：数据库的jdbc连接字符串
    - 必选：是
    - 字段类型：string
    - 默认值：无
      <br />

- **username**
    - 描述：数据源的用户名
    - 必选：是
    - 字段类型：string
    - 默认值：无
      <br />

- **password**
    - 描述：数据源指定用户名的密码
    - 必选：是
    - 字段类型：string
    - 默认值：无
      <br />

- **databaseName**
    - 描述：数据源数据库名称
    - 必选：是
    - 字段类型：string
    - 默认值：无
      <br />      

- **tableList**
    - 描述：需要解析的数据表。
    - 注意：指定此参数后filter参数将无效,table和filter都为空，监听jdbcUrl里的schema下所有表
    - 必选：否
    - 字段类型：list<string>
    - 默认值：无
      <br />
      
- **slotName**
    - 描述：slot 名称
    - 必选：否
    - 字段类型：String
    - 默认值：true
      <br />

- **allowCreated**
    - 描述：是否自动创建slot
    - 必选：否
    - 字段类型：boolean
    - 默认值：false
      <br />

- **temporary**
    - 描述：是否是临时的slot
    - 必选：否
    - 字段类型：boolean
    - 默认值：false
      <br />
      
- **statusInterval**
    - 描述：心跳间隔
    - 必选：否
    - 字段类型：int
    - 默认值：10
      <br />

- **lsn**
    - 描述：位点信息
    - 必选：否
    - 字段类型：long
    - 默认值：0
      <br />

- **slotAvailable**
    - 描述：slot是否可用
    - 必选：否
    - 字段类型：boolean
    - 默认值：false
      <br />


##  六、数据类型
| 支持 | BIT |
| --- | --- |
|  | NULL、 BOOLEAN、 TINYINT、 SMALLINT、 INTEGER、 INTERVAL_YEAR_MONTH、 BIGINT|
|  | INTERVAL_DAY_TIME、 DATE、 TIME_WITHOUT_TIME_ZONE |
|  | TIMESTAMP_WITHOUT_TIME_ZONE、 TIMESTAMP_WITH_LOCAL_TIME_ZONE、 FLOAT |
|  | DOUBLE、 CHAR、 VARCHAR、 DECIMAL、 BINARY、 VARBINARY |
| 暂不支持 | 无 |


##  七、脚本示例
见项目内`chunjun-examples`文件夹。
