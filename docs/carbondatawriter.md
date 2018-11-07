# Carbondata写入插件（carbondatawriter）

## 1. 配置样例

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3,
        "bytes": 0
      },
      "errorLimit": {
        "record": 10000,
        "percentage": 100
      }
    },
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "111111",
            "column": [
              "v",
              "id"
            ],
            "connection": [
              {
                "table": [
                  "tt"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://rdos1:3306/hyf?useCursorFetch=true"
                ]
              }
            ],
            "splitPk": "id"
          }
        },
        "writer": {
          "name": "carbondatawriter",
          "parameter": {
            "username": "admin",
            "password": "admin",
            "column": [
              "c",
              "a"
            ],
            "batchSize": 1000,
            "connection": [
              {
                "jdbcUrl": "jdbc:hive2://rdos2:10000/db1",
                "table": [
                  "cum1"
                ]
              }
            ]
          }
        }
      }
    ]
  }
}


```

## 2. 参数说明

* **name**

 	* 描述：插件名，此处只能填carbondatawriter，否则Flinkx将无法正常加载该插件包。
	* 必选：是 <br />
	* 默认值：无 <br />

* **jdbcUrl**

	* 描述：针对carbondata数据库的jdbc连接字符串

	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：数据源的用户名 <br />

	* 必选：否 <br />

	* 默认值：空 <br />

* **password**

	* 描述：数据源指定用户名的密码 <br />

	* 必选：否 <br />

	* 默认值：空 <br />

* **column**

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。

	* 必选：是 <br />

	* 默认值：否 <br />

	* 默认值：无 <br />

* **table**

	* 描述：目的表的表名称。目前只支持配置单个表，后续会支持多表。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

	* 必选：是 <br />

	* 默认值：无 <br />


* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与Mysql的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />

* **preSql**

	* 描述：写入数据到目的表前，会先执行这里的一组标准语句。

	* 必选：否 <br />

	* 默认值：无 <br />

* **postSql**

	* 描述：写入数据到目的表后，会执行这里的一组标准语句。

	* 必选：否 <br />

	* 默认值：无 <br />



## 3. 数据类型

支持如下数据类型

* SMALLINT
* INT/INTEGER
* BIGINT
* DOUBLE
* DECIMAL
* FLOAT
* BYTE
* BOOLEAN
* STRING
* CHAR
* VARCHAR
* DATE
* TIMESTAMP


不支持如下数据类型

* arrays: ARRAY<data_type>
* structs: STRUCT<col_name : data_type COMMENT col_comment, ...>
* maps: MAP<primitive_type, data_type>