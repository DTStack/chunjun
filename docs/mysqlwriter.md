# MySQL写入插件（mysqlwriter）

## 1. 配置样例

```
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 4
            },
            "errorLimit": {
                "record": 0,
                "percentage": 10
            }
        },
        "content": [
            {
              "reader": {
                "parameter": {
                  "password": "abc123"
                  	"column": [
                    "col1",
                    "col2"
                  ],
                  "where": "id > 1",
                  "connection": [
                    {
                      "jdbcUrl": [
                        "jdbc:mysql://172.16.8.104:3306/test?charset=utf8"
                      ],
                      "table": [
                        "tb2"
                      ]
                    }
                  ],
                  "splitPk": "col1",
                  "username": "dtstack"
                },
                "name": "mysqlreader"
              },
               "writer": {
                    "name": "sqlserverwriter",
                    "parameter": {
                        "batchSize": 2048,
                        "username": "sa",
                        "password": "Dtstack201610!",
                        "column": [
                            "id",
                            "v"
                        ],
                        "preSql": [],
                        "postSql": [],
                        "writeMode": "replace",
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:sqlserver://172.16.10.46:1433;DatabaseName=dq",
                                "table": [
                                    "tb1"
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

 	* 描述：插件名，此处只能填mysqlwriter，否则Flinkx将无法正常加载该插件包。
	* 必选：是 <br />

	* 默认值：无 <br />

* **jdbcUrl**

	* 描述：针对mysql数据库的jdbc连接字符串

		jdbcUrl按照Mysql官方规范，并可以填写连接附件控制信息。具体请参看[Mysql官方文档](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)。

	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：数据源的用户名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：数据源指定用户名的密码 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。
	
	* 必选：是 <br />

	* 默认值：否 <br />

	* 默认值：无 <br />

* **preSql**

	* 描述：写入数据到目的表前，会先执行这里的一组标准语句。
	
	* 必选：否 <br />

	* 默认值：无 <br />

* **postSql**

	* 描述：写入数据到目的表后，会执行这里的一组标准语句。

	* 必选：否 <br />

	* 默认值：无 <br />

* **table**

	* 描述：目的表的表名称。目前只支持配置单个表，后续会支持多表。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

	* 必选：是 <br />

	* 默认值：无 <br />

* **writeMode**

	* 描述：控制写入数据到目标表采用 `insert into` 或者 `replace into` 或者 `ON DUPLICATE KEY UPDATE` 语句<br />

	* 必选：是 <br />
	
	* 所有选项：insert/replace/update <br />

	* 默认值：insert <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与Mysql的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />