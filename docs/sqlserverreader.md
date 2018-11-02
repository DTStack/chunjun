# SQLServer读取插件（sqlserverreader）

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
          "name": "sqlserverreader",
          "parameter": {
            "username": "sa",
            "password": "Dtstack201610!",
            "column": [
              "id",
              "v"
            ],
            "where": "id > 1",
            "connection": [
              {
                "table": [
                  "tb1"
                ],
                "jdbcUrl": [
                  "jdbc:jtds:sqlserver://172.16.10.46:1433;DatabaseName=dq"
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

 	* 描述：插件名，此处只能填sqlserverreader，否则FlinkX将无法正常加载该插件包。
	* 必选：是 <br />

	* 默认值：无 <br />

* **jdbcUrl**

	* 描述：针对SQLServer数据库的jdbc连接字符串

	 jdbcUrl按照SqlServer官方规范，并可以填写连接附件控制信息。具体请参看[SqlServer官方文档](http://technet.microsoft.com/zh-cn/library/ms378749(v=SQL.110).aspx)。

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
	
* **where**

	* 描述：筛选条件，sqlserverreader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

          where条件可以有效地进行业务增量同步。如果不填写where语句，包括不提供where的key或者value，FlinkX均视作同步全量数据。

	* 必选：否 <br />

	* 默认值：无 <br />
	
* **splitPk**

	* 描述：sqlserverreader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，FlinkX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

	  推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

	  目前splitPk仅支持整形数据切分，`不支持浮点、字符串、日期等其他类型`。如果用户指定其他非支持类型，MysqlReader将报错！

	  如果splitPk不填写，包括不提供splitPk或者splitPk值为空，FlinkX视作使用单通道同步该表数据。

	* 必选：否 <br />

	* 默认值：空 <br />



* **column**

	* 描述：所配置的表中需要同步的列名集合。
	
	  支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。
      
      暂不支持常量列。

	* 必选：是 <br />

	* 默认值：无 <br />
	
