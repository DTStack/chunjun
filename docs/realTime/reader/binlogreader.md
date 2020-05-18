# MySQL Binlog Reader

<a name="c6v6n"></a>
## 一、插件名称
名称：**binlogreader**<br />

<a name="jVb3v"></a>
## 二、支持的数据源版本
**MySQL 5.X**<br />

<a name="2lzA4"></a>
## 三、数据库配置
**1.修改配置文件**
```sql
server_id=109
log_bin = /var/lib/mysql/mysql-bin
binlog_format = ROW
expire_logs_days = 30
```

<br />**2.添加权限**
```sql
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%' IDENTIFIED BY 'canal';
```
<br />
<a name="4yaqQ"></a>
## 四、参数说明<br />

- **jdbcUrl**
  - 描述：MySQL数据库的jdbc连接字符串，参考文档：[Mysql官方文档](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)
  - 必选：是
  - 默认值：无



- **username**
  - 描述：数据源的用户名
  - 必选：是
  - 默认值：无



- **password**
  - 描述：数据源指定用户名的密码
  - 必选：是
  - 默认值：无



- **host**
  - 描述：启动MySQL slave的机器ip
  - 必选：是
  - 默认值：无



- **port**
  - 描述：启动MySQL slave的端口
  - 必选：否
  - 默认值：3306



- **table**
  - 描述：需要解析的数据表。
  - 注意：指定此参数后filter参数将无效
  - 必选：否
  - 默认值：无



- **filter**
  - 描述：过滤表名的Perl正则表达式
  - 例子：
    - 所有表：`_.*_`
    - canal schema下所有表： `canal\..*`
    - canal下的以canal打头的表：`canal\.canal.*`
    - canal schema下的一张表：`canal\.test1`
  - 必选：否
  - 默认值：无



- **cat**
  - 描述：需要解析的数据更新类型，包括insert、update、delete三种
  - 注意：以英文逗号分割的格式填写。
  - 必选：否
  - 默认值：无



- **start**
  - 描述：要读取的binlog文件的开始位置
  - 参数：
    - journalName：采集起点按文件开始时的文件名称；
    - timestamp：采集起点按时间开始时的时间戳；
  - 默认值：无



- **pavingData**
  - 描述：是否将解析出的json数据拍平
  - 示例：假设解析的表为tb1,数据库为test，对tb1中的id字段做update操作，id原来的值为1，更新后为2，则pavingData为true时数据格式为：
```json
{
    "type":"update",
    "schema":"test",
    "table":"tb1",
    "ts":1231232,
    "ingestion":123213,
    "before_id":1,
    "after_id":2
}
```

<br />pavingData为false时：
```json
{
    "message":{
         "type":"update",
         "schema":"test",
         "table":"tb1",
         "ts":1231232,
         "ingestion":123213,
         "before":{
             "id":1
         },
         "after":{
             "id":2
         }
    }
}
```
其中”ts“是数据变更时间，ingestion是插件解析这条数据的纳秒时间

  - 必选：否
  - 默认值：false



- **bufferSize**
  - 描述：并发缓存大小
  - 注意：必须为2的幂
  - 必选：否
  - 默认值：1024
<a name="1LBc2"></a>
## 五、配置示例
<a name="stR2V"></a>
#### 1、单表监听
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "schema" : "tudou",
          "password" : "abc123",
          "cat" : "insert,delete,update",
          "jdbcUrl" : "jdbc:mysql://kudu3:3306/tudou",
          "host" : "kudu3",
          "start" : {
          },
          "table" : [ "binlog" ],
          "pavingData" : true,
          "username" : "dtstack"
        },
        "name" : "binlogreader"
      },
      "writer" : {
        "parameter" : {
          "print" : true
        },
        "name" : "streamwriter"
      }
    } ],
    "setting" : {
      "restore" : {
        "isRestore" : false,
        "isStream" : true
      },
      "errorLimit" : { },
      "speed" : {
        "bytes" : 0,
        "channel" : 1
      },
      "log" : {
        "isLogger": false,
        "level" : "trace",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```
<a name="3vQmm"></a>
#### 2、多表监听
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "schema" : "tudou",
          "password" : "abc123",
          "cat" : "insert,delete,update",
          "jdbcUrl" : "jdbc:mysql://kudu3:3306/tudou",
          "host" : "kudu3",
          "start" : {
          },
          "table" : ["kudu1", "kudu2"],
          "filter" : "",
          "pavingData" : true,
          "username" : "dtstack"
        },
        "name" : "binlogreader"
      },
      "writer" : {
        "parameter" : {
          "print" : true
        },
        "name" : "streamwriter"
      }
    } ],
    "setting" : {
      "restore" : {
        "isRestore" : false,
        "isStream" : true
      },
      "errorLimit" : { },
      "speed" : {
        "bytes" : 0,
        "channel" : 1
      },
      "log" : {
        "isLogger": false,
        "level" : "trace",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```
<a name="RVVSH"></a>
#### 3、正则监听
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "schema" : "tudou",
          "password" : "abc123",
          "cat" : "insert,delete,update",
          "jdbcUrl" : "jdbc:mysql://kudu3:3306/tudou",
          "host" : "kudu3",
          "start" : {
          },
          "filter" : "tudou\\.kudu.*",
          "pavingData" : true,
          "username" : "dtstack"
        },
        "name" : "binlogreader"
      },
      "writer" : {
        "parameter" : {
          "print" : true
        },
        "name" : "streamwriter"
      }
    } ],
    "setting" : {
      "restore" : {
        "isRestore" : false,
        "isStream" : true
      },
      "errorLimit" : { },
      "speed" : {
        "bytes" : 0,
        "channel" : 1
      },
      "log" : {
        "isLogger": false,
        "level" : "trace",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```
<a name="b08zE"></a>
#### 4、指定起始位置
```json
{
  "job" : {
    "content" : [ {
      "reader" : {
        "parameter" : {
          "schema" : "tudou",
          "password" : "abc123",
          "cat" : "insert,delete,update",
          "jdbcUrl" : "jdbc:mysql://kudu3:3306/tudou",
          "host" : "kudu3",
          "start" : {
            "journalName": "mysql-bin.000002",
            "timestamp" : 1589353414000
          },
          "table" : ["kudu"],
          "pavingData" : true,
          "username" : "dtstack"
        },
        "name" : "binlogreader"
      },
      "writer" : {
        "parameter" : {
          "print" : true
        },
        "name" : "streamwriter"
      }
    } ],
    "setting" : {
      "restore" : {
        "isRestore" : false,
        "isStream" : true
      },
      "errorLimit" : { },
      "speed" : {
        "bytes" : 0,
        "channel" : 1
      },
      "log" : {
        "isLogger": false,
        "level" : "trace",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```


<a name="ElEth"></a>
## 六、问题排查
采集mysql binlog 发现采集不到数据<br />1、查看binlog是否开启<br />       `show variables like '%log_bin%' ; ` <br />2、binlog_format 是否设置为ROW<br />        注意 binlog_format 必须设置为 ROW, 因为在 STATEMENT 或 MIXED 模式下, Binlog 只会记录和传输 SQL 语句（以减少日志大小），而不包含具体数据，我们也就无法保存了。<br />3、从节点通过一个专门的账号连接主节点，这个账号需要拥有全局的 REPLICATION 权限。我们可以使用 GRANT 命令创建这样的账号：<br />     GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT<br />    ON *.* TO 'canal'@'%' IDENTIFIED BY 'canal';<br />  参考：[https://blog.csdn.net/zjerryj/article/details/77152226](https://blog.csdn.net/zjerryj/article/details/77152226)
