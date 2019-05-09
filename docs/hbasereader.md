# HBase读取插件（hbasereader）

## 1. 配置样例

```
{
	"job": {
		"setting": {},
		"content": [{
			"reader": {
				"name": "hbasereader",
				"parameter": {
					"hbaseConfig": {
						"hbase.zookeeper.property.clientPort": "2181",
						"hbase.rootdir": "hdfs://ns1/hbase",
						"hbase.cluster.distributed": "true",
						"hbase.zookeeper.quorum": "host1,host2,host3",
						"zookeeper.znode.parent": "/hbase"
					},
					"table": "tableTest",
					"encodig": "utf-8",
					"column": [{
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
			"writer": {}
		}]
	}
}
```

## 2. 参数说明

* **hbaseConfig**
  
  * 描述：hbase的连接配置，以json的形式组织 (见hbase-site.xml)
  
  * 必选：是
  
  * 默认值：无

* **encoding**
  
  * 描述：字符编码
  
  * 必选：无 
  
  * 默认值：utf-8 

* **table**
  
  * 描述：hbase表名
  
  * 必选：是 
  
  * 默认值：无 

* **range**
  
  * 描述：指定hbasereader读取的rowkey范围。   
    
    * startRowkey：指定开始rowkey；
    
    * endRowkey指定结束rowkey；
    
    * isBinaryRowkey：指定配置的startRowkey和endRowkey转换为byte[]时的方式，默认值为false,若为true，则调用Bytes.toBytesBinary(rowkey)方法进行转换;若为false：则调用Bytes.toBytes(rowkey)，配置格式如下：
      
      ```
      "range": {
       "startRowkey": "aaa",
       "endRowkey": "ccc",
       "isBinaryRowkey":false
      }
      ```
  
  * 必选：否 
  
  * 默认值：无 

* **column**
  
  * 描述：要读取的hbase字段，normal 模式与multiVersionFixedColumn 模式下必填项。
    
    * name：指定读取的hbase列，除了rowkey外，必须为 列族:列名 的格式；
    
    * type：指定源数据的类型，format指定日期类型的格式，value指定当前类型为常量，不从hbase读取数据，而是根据value值自动生成对应的列。
  
  * 必选：是
  
  * 默认值：无 
