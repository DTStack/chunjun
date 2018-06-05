# MySQL写入插件（mysqlwriter）

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
            },
            "dirty": {
                "path": "/tmp",
                "hadoopConfig": {
                    "fs.default.name": "hdfs://ns1",
                    "dfs.nameservices": "ns1",
                    "dfs.ha.namenodes.ns1": "nn1,nn2",
                    "dfs.namenode.rpc-address.ns1.nn1": "node02:9000",
                    "dfs.namenode.rpc-address.ns1.nn2": "node03:9000",
                    "dfs.ha.automatic-failover.enabled": "true",
                    "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                    "fs.hdfs.impl.disable.cache": "true"
                }
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "username": "dtstack",
                        "password": "abc123",
                        "column": [
                            "id",
                            "v1"
                        ],
                        "where": "id > 1",
                        "connection": [
                            {
                                "table": [
                                    "sb9"
                                ],
                                "jdbcUrl": [
                                    "jdbc:mysql://172.16.8.104:3306/test?useCursorFetch=true"
                                ]
                            }
                        ],
                        "splitPk": "id"
                    }
                },
               "writer": {
                    "name": "mysqlwriter",
                    "parameter": {
                        "writeMode": "insert",
                        "username": "dtstack",
                        "password": "abc123",
                        "column": [
                            "c1",
                            "c2"
                        ],
                        "batchSize": 1,
                        "session": [
                            "set session sql_mode='ANSI'"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://172.16.8.104:3306/test?useCursorFetch=true",
                                "table": [
                                    "tb3"
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