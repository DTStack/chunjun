# HDFS写入插件（hdfswriter）

## 1. 配置样例

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "parameter": {
            "password": "abc123",
            "columnTypes": [
              "java.lang.Integer",
              "java.lang.String"
            ],
            "column": [
              "col1",
              "col2"
            ],
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
          "name": "hdfswriter",
          "parameter": {
            "hadoopConfig": {
                "dfs.nameservices":"ns1",
                "dfs.ha.namenodes.ns1": "nn1,nn2",
                "dfs.namenode.rpc-address.ns1.nn1": "node02:9000",
                "dfs.namenode.rpc-address.ns1.nn2": "node03:9000",
                "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            },
            "defaultFS": "hdfs://ns1",
            "fileType": "text",
            "fileName": "hallo",
            "column": [
              {
                "name": "col1",
                "type": "STRING"
              },
              {
                "name": "col2",
                "type": "STRING"
              }
            ],
            "path": "/hyf",
            "writeMode": "append",
            "fieldDelimiter": "\\001"
          }
        }
      }
    ]
  }
}
```

## 2. 参数说明

* **defaultFS**

	* 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:9000<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **fileType**

	* 描述：文件的类型，目前只支持用户配置为"text"或"orc"。 <br />

		text表示textfile文件格式

		orc表示orcfile文件格式

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **path**

	* 描述：存储到Hadoop hdfs文件系统的路径信息，HdfsWriter会根据并发配置在Path目录下写入多个文件。为与hive表关联，请填写hive表在hdfs上的存储路径。例：Hive上设置的数据仓库的存储路径为：/user/hive/warehouse/ ，已建立数据库：test，表：hello；则对应的存储路径为：/user/hive/warehouse/test.db/hello  <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **fileName**

 	* 描述：HdfsWriter写入时的文件名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **column**

	* 描述：写入数据的字段，不支持对部分列写入。为与hive中表关联，需要指定表中所有字段名和字段类型，其中：name指定字段名，type指定字段类型。 <br />

		```json
		"column":
                 [
                            {
                                "name": "userName",
                                "type": "string"
                            },
                            {
                                "name": "age",
                                "type": "long"
                            }
                 ]
		```

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **writeMode**

 	* 描述：hdfswriter写入前数据清理处理模式： <br />

		* append，追加
		* overwrite，覆盖

	* 必选：是 <br />

	* 默认值：无 <br />

* **fieldDelimiter**

	* 描述：hdfswriter写入时的字段分隔符,**需要用户保证与创建的Hive表的字段分隔符一致，否则无法在Hive表中查到数据** <br />

	* 必选：是 <br />

	* 默认值：\\001 <br />

* **compress**

	* 描述：hdfs文件压缩类型，默认不填写意味着没有压缩。其中：text类型文件支持压缩类型有gzip、bzip2;orc类型文件支持的压缩类型有NONE、SNAPPY（需要用户安装SnappyCodec）。 <br />

	* 必选：否 <br />

	* 默认值：无压缩 <br />

* **encoding**

	* 描述：写文件的编码配置。<br />

 	* 必选：否 <br />

 	* 默认值：utf-8，**慎重修改** <br />
