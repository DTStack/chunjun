# Teradata Writer

<a name="c6v6n"></a>
## 一、插件名称
名称：**teradatawriter**<br />
<a name="jVb3v"></a>
## 二、支持的数据源版本
**Teradata 15.0及以上**<br />
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
  - 描述：控制写入数据到目标表采用 `insert into` 或者 `replace into` 或者 `ON DUPLICATE KEY UPDATE` 语句
  - 必选：是
  - 所有选项：只支持insert
  - 默认值：insert



- **batchSize**
  - 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与数据库的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况
  - 必选：否
  - 默认值：1024



- **updateKey**
  - 描述：当写入模式为update和replace时，需要指定此参数的值为唯一索引字段
  - 注意：
    - 如果此参数为空，并且写入模式为update和replace时，应用会自动获取数据库中的唯一索引；
    - 如果数据表没有唯一索引，但是写入模式配置为update和replace，应用会以insert的方式写入数据；
  - 必选：否
  - 默认值：无

**
<a name="1LBc2"></a>
## 四、配置示例
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "name": "id",
                "type": "id"
              },
              {
                "name": "user_id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              }
            ],
            "sliceRecordCount" : [ "100"]
          }
        },
        "writer": {
          "name": "teradatawriter",
          "parameter": {
            "username": "tudou",
            "password": "abc123",
            "connection": [
              {
                "jdbcUrl": "jdbc:teradata://kudu3/testbase",
                "table": ["Employee"]
              }
            ],
            "preSql": ["delete from TUDOU.KUDU"],
            "postSql": ["update TUDOU.KUDU set USER_ID = 1"],
            "writeMode": "insert",
            "column": ["ID","USER_ID","NAME"],
            "batchSize": 1024
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
        "record": 1
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "log" : {
        "isLogger": false,
        "level" : "debug",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```
