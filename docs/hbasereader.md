# HBase读取插件（hbasereader）

## 1. 配置样例

```
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 2,
                 "bytes": 10000
            },
            "errorLimit": {
                "record": 0,
                "percentage": 2
            }
        },
        "content": [
            {
                "reader": {
                    "name": "hbasereader",
                    "parameter": {
                        "hbaseConfig": {
                            "hbase.zookeeper.property.clientPort": "2181",
                            "hbase.rootdir": "hdfs://ns1/hbase",
                            "hbase.cluster.distributed": "true",
                            "hbase.zookeeper.quorum": "node01,node02,node03",
                            "zookeeper.znode.parent": "/hbase"
                        },
                        "table": "sb5",
                        "encodig": "utf-8",
                        "column": [
                            {
                                "name": "rowkey",
                                "type": "string"
                            },
                            {
                                "name": "cf1:id",
                                "type": "string"
                            }
                        ],
                        "range": {
                            "startRowkey": "",
                            "endRowkey": "",
                            "isBinaryRowkey": true
                        }
                    }
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
        ]
    }
}

```

## 2. 参数说明

* **hbaseConfig**

	* 描述：hbase的连接配置，以json的形式组织 (见hbase-site.xml)<br />

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **encoding**

	* 描述：字符编码<br />

	* 必选：无 <br />

	* 默认值：utf-8 <br />
	
* **table**

	* 描述：hbase表名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **range**

	* 描述：指定hbasereader读取的rowkey范围。<br />   
	startRowkey：指定开始rowkey；<br />   
	endRowkey指定结束rowkey；<br />  
	isBinaryRowkey：指定配置的startRowkey和endRowkey转换为byte[]时的方式，默认值为false,若为true，则调用Bytes.toBytesBinary(rowkey)方法进行转换;若为false：则调用Bytes.toBytes(rowkey)<br />  
	配置格式如下：   
	
	```
	"range": {
  		"startRowkey": "aaa",
  		"endRowkey": "ccc",
  		"isBinaryRowkey":false
}
	```
	<br />
	  
	* 必选：否 <br />
 
	* 默认值：无 <br />

* **column**

	* 描述：要读取的hbase字段，normal 模式与multiVersionFixedColumn 模式下必填项。    
	name指定读取的hbase列，除了rowkey外，必须为 列族:列名 的格式，type指定源数据的类型，format指定日期类型的格式，value指定当前类型为常量，不从hbase读取数据，而是根据value值自动生成对应的列。配置格式如下：
	
	```
	"column": 
[
	{
	    "name": "rowkey",
	    "type": "string"
	},
	{
	    "value": "test",
	    "type": "string"
	}
] 	
		            
	```

	* 必选：是<br />
 
	* 默认值：无 <br />