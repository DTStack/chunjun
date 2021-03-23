# ClickHouse Writer

<a name="c6v6n"></a>
## 一、插件名称
名称：**clickhousewriter**<br />
<a name="lI3mV"></a>
## 二、支持的数据源版本
**ClickHouse 19.x及以上**<br />

<a name="2lzA4"></a>
## 三、参数说明

- **connection**
  - 描述：数据库连接参数，包含jdbcUrl、schema、table等参数
  - 必选：是
  - 字段类型：List
    - 示例：指定jdbcUrl、schema、table
  ```json
  "connection": [{
       "jdbcUrl": "jdbc:clickhouse://localhost:8123/database",
       "table": ["table"],
       "schema":"public"
      }] 
   ```
  - 默认值：无

<br/>

- **jdbcUrl**
  - 描述：针对关系型数据库的jdbc连接字符串
  - 必选：是
  - 字段类型：String
  - 默认值：无
  
<br/>      

- **schema**
  - 描述：数据库schema名
  - 必选：否
  - 字段类型：String
  - 默认值：无

<br/> 
  
- **table**
  - 描述：目的表的表名称。目前只支持配置单个表，后续会支持多表
  - 必选：是
  - 字段类型：List
  - 默认值：无

<br/>      

- **username**
  - 描述：数据源的用户名
  - 必选：是
  - 字段类型：String
  - 默认值：无

<br/>  

- **password**
  - 描述：数据源指定用户名的密码
  - 必选：是
  - 字段类型：String
  - 默认值：无

<br/>  

- **column**
  - 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。
  - 必选：是
  - 默认值：否
  - 字段类型：List
  - 默认值：无

<br/>  

- **fullcolumn**
  - 描述：目的表中的所有字段，字段之间用英文逗号分隔。例如: "column": ["id","name","age","hobby"]，如果不配置，将在系统表中获取
  - 必选：否
  - 字段类型：List
  - 默认值：无

<br/>

- **preSql**
  - 描述：写入数据到目的表前，会先执行这里的一组标准语句
  - 必选：否
  - 字段类型：String
  - 默认值：无

<br/>  

- **postSql**
  - 描述：写入数据到目的表后，会执行这里的一组标准语句
  - 必选：否
  - 字段类型：String
  - 默认值：无

<br/>  

- **writeMode**
  - 描述：控制写入数据到目标表采用 `insert into` 语句，只支持insert操作
  - 必选：是
  - 所有选项：insert
  - 字段类型：String
  - 默认值：insert

<br/>  

- **batchSize**
  - 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与数据库的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况
  - 必选：否
  - 字段类型：int
  - 默认值：1024
  
<br/>


<a name="1LBc2"></a>
## 四、配置示例
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
              "name" : "name",
              "type" : "string"
            },
            {
              "name" : "eventDate",
              "type" : "date"
            }
          ]
        },
        "name": "streamreader"
      },
      "writer": {
        "name": "clickhousewriter",
        "parameter": {
          "connection": [{
            "jdbcUrl": "jdbc:clickhouse://localhost:8123/database",
            "table": ["test"]
          }],
          "username": "username",
          "password": "password",
          "column": ["id","user_id","name","eventDate"],
          "writeMode": "insert",
          "batchSize": 1024,
          "preSql": [],
          "postSql": []
        }
      }
    }],
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


