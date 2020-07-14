# Greenplum Writer

<a name="c6v6n"></a>
## 一、插件名称
名称：**greenplumwriter**<br />
<a name="841YG"></a>
## 二、支持的数据源版本
**Greenplum 5及以上**<br />**
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
   - 描述：仅支持`insert`操作，可以搭配insertSqlMode使用
   - 必选：是
   - 默认值：无，



- **insertSqlMode**
   - 描述：控制写入数据到目标表采用  `COPY table_name [ ( column_name [, ...] ) ] FROM STDIN DELIMITER 'delimiter_character'`语句，提高数据的插入效率
   - 注意：
      - 为了避免`insert`过慢带来的问题，此参数被固定为`copy`
      - 当指定此参数时，writeMode的值必须为 `insert`，否则设置无效
   - 必选：否
   - 默认值：无



- **batchSize**
   - 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与数据库的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况
   - 必选：否
   - 默认值：1024

**
<a name="1LBc2"></a>
## 四、配置示例
<a name="QBFSI"></a>
#### 1、insert with copy mode
```json
{
  "job": {
    "content": [{
      "reader": {
        "parameter": {
          "column": [
            {
              "name": "id",
              "type": "int",
              "value": 1
            }
          ],
          "sliceRecordCount": ["100"]
        },
        "name" : "streamreader"
      },
      "writer": {
        "name": "greenplumwriter",
        "parameter": {
          "connection": [{
            "jdbcUrl": "jdbc:pivotal:greenplum://localhost:5432;DatabaseName=exampledb",
            "table": ["tbl_pay_log_copy"]
          }],
          "username": "gpadmin",
          "password": "gpadmin",
          "column": [
            {
              "name": "id",
              "type": "int"
            }],
          "writeMode": "insert",
          "insertSqlMode": "copy",
          "batchSize": 100,
          "preSql": ["TRUNCATE tbl_pay_log_copy"],
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
        "record": 100
      }
    }
  }
}
```
