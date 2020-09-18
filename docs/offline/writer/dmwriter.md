# DM Writer

<a name="c6v6n"></a>
## 一、插件名称
名称：**dmwriter**
<a name="jVb3v"></a>
## 二、支持的数据源版本
**DM7、DM8**<br />
<a name="2lzA4"></a>
## 三、参数说明

- **jdbcUrl**
  - 描述：针对关系型数据库的jdbc连接字符串
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



- **preSql**
  - 描述：写入数据到目的表前，会先执行这里的一组标准语句
  - 必选：否
  - 默认值：无



- **postSql**
  - 描述：写入数据到目的表后，会执行这里的一组标准语句
  - 必选：否
  - 默认值：无



- **table**
  - 描述：目的表的表名称。目前只支持配置单个表，后续会支持多表
  - 必选：是
  - 默认值：无



- **writeMode**
  - 描述：控制写入数据到目标表采用 `insert into` 或者 `merge into`  语句
  - 必选：是
  - 所有选项：insert/update
  - 默认值：insert



- **batchSize**
  - 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与数据库的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况
  - 必选：否
  - 默认值：1024



- **updateKey**
  - 描述：当写入模式为update时，需要指定此参数的值为唯一索引字段
  - 注意：
    - 采用`merge into`语法，对目标表进行匹配查询，匹配成功时更新，不成功时插入；
  - 必选：否
  - 默认值：无

**
<a name="1LBc2"></a>
## 四、配置示例
<a name="2XWsI"></a>
#### 1、insert
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "sliceRecordCount": ["100"],
            "column": [
              {
                "name": "id",
                "type": "int"
              },
              {
                "name": "age",
                "type": "int"
              }
            ]
          },
          "name": "streamreader"
        },
        "writer": {
          "name": "dmwriter",
          "parameter": {
            "username": "SYSDBA",
            "password": "SYSDBA",
            "connection": [
              {
                "jdbcUrl": "jdbc:dm://localhost:5236",
                "table": [
                  "PERSON.STUDENT"
                ]
              }
            ],
            "session": [],
            "preSql": [],
            "postSql": [],
            "writeMode": "insert",
            "column": [
              {
                "name": "ID",
                "type": "int"
              },
              {
                "name": "AGE",
                "type": "int"
              }
            ]
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      }
    }
  }
}
```
<a name="kjyKG"></a>
#### 2、update
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "sliceRecordCount": ["1"],
            "column": [
              {
                "name": "int",
                "type": "int",
                "value": "3"
              },
              {
                "name": "age",
                "type": "int",
                "value": "3"
              }
            ]
          },
          "name": "streamreader"
        },
        "writer": {
          "name": "dmwriter",
          "parameter": {
            "username": "SYSDBA",
            "password": "SYSDBA",
            "connection": [
              {
                "jdbcUrl": "jdbc:dm://localhost:5236",
                "table": [
                  "PERSON.STUDENT"
                ]
              }
            ],
            "session": [],
            "preSql": [],
            "postSql": [],
            "writeMode": "update",
            "updateKey": {"key": ["ID"]},
            "column": [
              {
                "name": "ID",
                "type": "int"
              },
              {
                "name": "AGE",
                "type": "int"
              }
            ]
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      }
    }
  }
}
```
