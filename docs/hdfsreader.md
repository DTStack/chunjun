# HDFS读取插件（hdfsreader）

## 1. 配置样例

```
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "path": "hdfs://ns1/user/hive/warehouse/wujing_test.db/kepa_250",
            "hadoopConfig": {
              "dfs.ha.namenodes.ns1": "nn1,nn2",
              "dfs.namenode.rpc-address.ns1.nn2": "node03:9000",
              "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "dfs.namenode.rpc-address.ns1.nn1": "node02:9000",
              "dfs.nameservices": "ns1"
            },
            "column": [
              {
                "name": "col1",
                "index": 0,
                "type": "string"
              },
              {
                "name": "col2",
                "index": 1,
                "type": "string"
              }
            ],
            "defaultFS": "hdfs://ns1",
            "fieldDelimiter": "",
            "encoding": "utf-8",
            "fileType": "orc"
          },
          "name": "hdfsreader"
        },
        "writer": {
          "parameter": {
            "password": "abc123",
            "column": [
              "col1",
              "col2"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://172.16.8.104:3306/test?charset=utf8",
                "table": [
                  "sb5"
                ]
              }
            ],
            "writeMode": "insert",
            "username": "dtstack"
          },
          "name": "mysqlwriter"
        }
      }
    ],
    "setting": {
      "errorLimit": {
        "record": 100
      },
      "speed": {
        "bytes": 1048576,
        "channel": 1
      }
    }
  }
}
```


## 2. 参数说明

* **path**

	* 描述：要读取的文件路径，多个路径可以用逗号隔开

	* 必选：是 <br />

	* 默认值：无 <br />

* **defaultFS**

	* 描述：Hadoop hdfs文件系统namenode节点地址。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **fileType**

	* 描述：文件的类型，目前只支持用户配置为"text"、"orc" <br />

		text表示textfile文件格式

		orc表示orcfile文件格式

	* 必选：是 <br />

	* 默认值：无 <br />


* **column**

	* 描述：读取字段列表，type指定源数据的类型，

		```json
{
  "type": "long",
  "index": 0    //从本地文件文本第一列获取int字段
},
{
  "type": "string",
  "value": "yesyoucan"  //HdfsReader内部生成yesyoucan的字符串字段作为当前字段
}
		```

		对于用户指定Column信息，type必须填写，index/value必须选择其一。

	* 必选：是 <br />

	* 默认值：全部按照string类型读取 <br />

* **fieldDelimiter**

	* 描述：读取的字段分隔符 <br />

	**另外需要注意的是，HdfsReader在读取textfile数据时，需要指定字段分割符，HdfsReader在读取orcfile时，用户无需指定字段分割符**

	* 必选：否 <br />

	* 默认值：\\001 <br />


* **encoding**

	* 描述：读取文件的编码配置。<br />

 	* 必选：否 <br />

 	* 默认值：utf-8 <br />
	
* **hadoopConfig**

	* 描述：hadoopConfig里可以配置与Hadoop相关的一些高级参数，比如HA的配置。<br />

		```json
		"hadoopConfig":{
		        "dfs.nameservices": "testDfs",
		        "dfs.ha.namenodes.testDfs": "namenode1,namenode2",
		        "dfs.namenode.rpc-address.aliDfs.namenode1": "",
		        "dfs.namenode.rpc-address.aliDfs.namenode2": "",
		        "dfs.client.failover.proxy.provider.testDfs": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
		}
		```

	* 必选：否 <br />
 
 	* 默认值：无 <br />


