# MySQL Binlog Reader

<!-- TOC -->

- [MySQL Binlog Reader](#mysql-binlog-reader)
    - [一、插件名称](#一插件名称)
    - [二、支持的数据源版本](#二支持的数据源版本)
    - [三、数据库配置](#三数据库配置)
        - [1.修改配置文件](#1修改配置文件)
        - [2.添加权限](#2添加权限)
    - [四、参数说明](#四参数说明)
    - [五、配置示例](#五配置示例)
        - [1、单表监听](#1单表监听)
        - [2、多表监听](#2多表监听)
        - [3、正则监听](#3正则监听)
        - [4、指定起始位置](#4指定起始位置)

<!-- /TOC -->

<br/>

## 一、插件名称
名称：**binlogreader**

<br/>

## 二、支持的数据源版本
**MySQL5.1.5及以上**

<br/>

## 三、数据库配置
### 1.修改配置文件
binlog_format需要修改为 ROW 格式，在/etc/my.cnf文件里[mysqld]下添加下列配置
```sql
server_id=109
log_bin = /var/lib/mysql/mysql-bin
binlog_format = ROW
expire_logs_days = 30
```


### 2.添加权限
mysql binlog权限需要三个权限 SELECT, REPLICATION SLAVE, REPLICATION CLIENT
```sql
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%' IDENTIFIED BY 'canal';
```



- 缺乏SELECT权限时，报错为
```
com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException:
Access denied for user 'canal'@'%' to database 'binlog'
```

- 缺乏REPLICATION SLAVE权限时，报错为
```
java.io.IOException: 
Error When doing Register slave:ErrorPacket [errorNumber=1045, fieldCount=-1, message=Access denied for user 'canal'@'%'
```

- 缺乏REPLICATION CLIENT权限时，报错为
```
 com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException:
        Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation
```


binlog为什么需要这些权限：

- Select权限代表允许从表中查看数据
- Replication client权限代表允许执行show master status,show slave status,show binary logs命令
- Replication slave权限代表允许slave主机通过此用户连接master以便建立主从 复制关系

<br/>

## 四、参数说明


- **jdbcUrl**
  - 描述：MySQL数据库的jdbc连接字符串，参考文档：[Mysql官方文档](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **username**
  - 描述：数据源的用户名
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **password**
  - 描述：数据源指定用户名的密码
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **host**
  - 描述：启动MySQL slave的机器ip
  - 必选：是
  - 字段类型：string
  - 默认值：无

<br/>

- **port**
  - 描述：启动MySQL slave的端口
  - 必选：否
  - 字段类型：int
  - 默认值：3306

<br/>

- **table**
  - 描述：需要解析的数据表。
  - 注意：指定此参数后filter参数将无效,table和filter都为空，监听jdbcUrl里的schema下所有表
  - 必选：否
  - 字段类型：list<string>
  - 默认值：无

<br/>

- **filter**
  - 描述：过滤表名的Perl正则表达式
  - 注意：table和filter都为空，监听jdbcUrl里的schema下所有表
  - 必选：否
  - 字段类型：string
  - 默认值：无
  - 例子：
    - 所有表：`_.*_`
    - canal schema下所有表： `canal\..*`
    - canal下的以canal打头的表：`canal\.canal.*`
    - canal schema下的一张表：`canal\.test1`

<br/>

- **cat**
  - 描述：需要解析的数据更新类型，包括insert、update、delete三种
  - 注意：以英文逗号分割的格式填写。如果为空，解析所有数据更新类型
  - 必选：否
  - 字段类型：string
  - 默认值：无

<br/>

- **start**
  - 描述：要读取的binlog文件的开始位置
  - 注意：为空，则从当前position处消费，timestamp的优先级高于 journalName+position
  - 参数：
    - timestamp：时间戳，采集起点从指定的时间戳处消费；
    - journalName：文件名，采集起点从指定文件的起始处消费；
    - position：文件的指定位置，采集起点从指定文件的指定位置处消费
  - 字段类型：map
  - 默认值：无

<br/>

- **pavingData**
  - 描述：是否将解析出的json数据拍平
  - 必选：否
  - 字段类型：boolean
  - 默认值：true
  - 示例：假设解析的表为tb1,数据库为test，对tb1中的id字段做update操作，id原来的值为1，更新后为2，则pavingData为true时，数据格式为：
```json
{
    "type":"update",
    "schema":"test",
    "table":"tb1",
    "ts":6760525407742726144,
    "before_id":1,
    "after_id":2
}
```
pavingData为false时：
```json
{
    "message":{
         "type":"update",
         "schema":"test",
         "table":"tb1",
         "ts":6760525407742726144,
         "before":{
             "id":1
         },
         "after":{
             "id":2
         }
    }
}
```

- type：变更类型，INSERT，UPDATE、DELETE
- ts：自增ID，不重复，可用于排序，解码后为FlinkX的事件时间，解码规则如下:
```java
    long id = Long.parseLong("6760525407742726144");
    long res = id >> 22;
    DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    System.out.println(sdf.format(res));		//2021-01-28 19:54:21
```

<br/>

- **slaveId**
  - 描述：从服务器的ID
  - 注意：同一个MYSQL复制组内不能重复
  - 必选：否
  - 字段类型：long
  - 默认值：new Object().hashCode()

<br/>

- **connectionCharset**
  - 描述：编码信息
  - 必选：否
  - 字段类型：string
  - 默认值：UTF-8

<br/>

- **detectingEnable**
  - 描述：是否开启心跳
  - 必选：否
  - 字段类型：boolean
  - 默认值：true

<br/>

- **detectingSQL**
  - 描述：心跳SQL
  - 必选：否
  - 字段类型：string
  - 默认值：SELECT CURRENT_DATE

<br/>

- **enableTsdb**
  - 描述：是否开启时序表结构能力
  - 必选：否
  - 字段类型：boolean
  - 默认值：true

<br/>

- **bufferSize**
  - 描述：并发缓存大小
  - 注意：必须为2的幂
  - 必选：否
  - 默认值：1024

<br/>

- **parallel**
  - 描述：是否开启并行解析binlog日志
  - 必选：否
  - 字段类型：boolean
  - 默认值：true

<br/>

- **parallelThreadSize**
  - 描述：并行解析binlog日志线程数
  - 注意：只有 paraller 设置为true才生效
  - 必选：否
  - 字段类型：int
  - 默认值：2

<br/>

- **isGTIDMode**
  - 描述：是否开启gtid模式
  - 必选：否
  - 字段类型：boolean
  - 默认值：false

<br/>

## 五、配置示例
### 1、单表监听
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "table": ["table"],
            "password": "passwd",
            "database": "database",
            "port": 3306,
            "cat": "DELETE,INSERT,UPDATE",
            "host": "host",
            "jdbcUrl": "jdbc:mysql://host:port/schema",
            "pavingData": true,
            "username": "name"
          },
          "name": "binlogreader"
        },
        "writer": {
          "parameter": {
            "print": true
          },
          "name": "streamwriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "isStream": true
      },
      "speed": {
        "channel": 1
      }
    }
  }
}
```
### 2、多表监听
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "table": ["table1","table2"],
            "password": "passwd",
            "database": "database",
            "port": 3306,
            "cat": "DELETE,INSERT,UPDATE",
            "host": "host",
            "jdbcUrl": "jdbc:mysql://host:port/schema",
            "pavingData": true,
            "username": "name"
          },
          "name": "binlogreader"
        },
        "writer": {
          "parameter": {
            "print": true
          },
          "name": "streamwriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "isStream": true
      },
      "speed": {
        "channel": 1
      }
    }
  }
}
```
### 3、正则监听
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "filter": "schema\\..*",
            "password": "passwd",
            "database": "database",
            "port": 3306,
            "cat": "DELETE,INSERT,UPDATE",
            "host": "host",
            "jdbcUrl": "jdbc:mysql://host:port/schema",
            "pavingData": true,
            "username": "name"
          },
          "name": "binlogreader"
        },
        "writer": {
          "parameter": {
            "print": true
          },
          "name": "streamwriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "isStream": true
      },
      "speed": {
        "channel": 1
      }
    }
  }
}
```
### 4、指定起始位置
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "filter": "schema\\..*",
            "password": "passwd",
            "database": "database",
            "port": 3306,
             "start" : {
              "journalName": "binlog.000031",
              "position": 4
            },
            "cat": "DELETE,INSERT,UPDATE",
            "host": "host",
            "jdbcUrl": "jdbc:mysql://host:port/schema",
            "pavingData": true,
            "username": "name"
          },
          "name": "binlogreader"
        },
        "writer": {
          "parameter": {
            "print": true
          },
          "name": "streamwriter"
        }
      }
    ],
    "setting": {
      "restore": {
        "isStream": true
      },
      "speed": {
        "channel": 1
      }
    }
  }
}
```


