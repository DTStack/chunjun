# SqlServer Writer

<a name="2lzA4"></a>
## 一、插件名称
名称：**sqlserverwriter**<br />
<a name="JSVBM"></a>
## 二、支持的数据源版本
**Microsoft SQL Server 2012及以上**<br />
<a name="hwps8"></a>
## 三、参数说明

- **jdbcUrl**
  - 描述：使用开源的jtds驱动连接 而非Microsoft的官方驱动<br />jdbcUrl参考文档：[jtds驱动官方文档](http://jtds.sourceforge.net/faq.html)
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



- **column**
  - 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]
  - 必选：是
  - 默认值：否
  - 默认值：无



- **presql**
  - 描述：写入数据到目的表前，会先执行这里的一组标准语句
  - 必选：否
  - 默认值：无

<br />

- **postSql**
  - 描述：写入数据到目的表后，会执行这里的一组标准语句
  - 必选：否
  - 默认值：无



- **table**
  - 描述：目的表的表名称。目前只支持配置单个表，后续会支持多表
  - 必选：是
  - 默认值：无



- **writeMode**
  - 描述：控制写入数据到目标表采用 `insert into` 或者` merge into` 语句
  - 必选：是
  - 所有选项：insert/update
  - 默认值：insert



- **updateKey**
  - 描述：当写入模式为update时，需要指定此参数的值为唯一索引字段
  - 注意：
    - 采用`merge into`语法，对目标表进行匹配查询，匹配成功时更新，不成功时插入；
  - 必选：否
  - 默认值：无



- **batchSize**
  - 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与数据库的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况
  - 必选：否
  - 默认值：1024


<br />**
<a name="1LBc2"></a>
## 四、配置示例
<a name="JKNLE"></a>
#### 1、insert
```json
{
  "job": {
    "content": [{
      "reader": {
        "parameter": {
          "sliceRecordCount": ["100"],
          "column": [
            {
              "name": "id",
              "type": "int"
            },
            {
              "name": "user_id",
              "type": "int"
            },
            {
              "name":"name",
              "type":"string"
            }
          ]
        },
        "name": "streamreader"
      },
      "writer": {
        "name": "sqlserverwriter",
        "parameter": {
          "connection": [{
            "jdbcUrl": "jdbc:jtds:sqlserver://0.0.0.1:1433;DatabaseName=DTstack",
            "table": [
              "tableTest"
            ]
          }],
          "username": "username",
          "password": "password",
          "column": [
            {
            "name": "id",
            "type": "BIGINT"
          },
            {
              "name": "user_id",
              "type": "BIGINT"
            },
            {
              "name": "name",
              "type": "varchar"
            }],
          "writeMode": "insert",
          "batchSize": 1024,
          "preSql": [],
          "postSql": [],
          "updateKey": {}
        }
      }
    }],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      }
    }
  }
}
```
<a name="pRwQ9"></a>
#### 2、update
```json
{
  "job": {
    "content": [{
      "reader": {
        "parameter": {
          "sliceRecordCount": ["100"],
          "column": [
            {
              "name": "id",
              "type": "int"
            },
            {
              "name": "user_id",
              "type": "int"
            },
            {
              "name":"name",
              "type":"string"
            }
          ]
        },
        "name": "streamreader"
      },
      "writer": {
        "name": "sqlserverwriter",
        "parameter": {
          "connection": [{
            "jdbcUrl": "jdbc:jtds:sqlserver://0.0.0.1:1433;DatabaseName=DTstack",
            "table": [
              "tableTest"
            ]
          }],
          "username": "username",
          "password": "password",
          "column": [
            {
            "name": "id",
            "type": "BIGINT"
          },
            {
              "name": "user_id",
              "type": "BIGINT"
            },
            {
              "name": "name",
              "type": "varchar"
            }],
          "writeMode": "update",
          "batchSize": 1024,
          "preSql": [],
          "postSql": [],
          "updateKey": {"key": ["id"]}
        }
      }
    }],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      }
    }
  }
}
```


