# Elasticsearch读取插件（esreader）

## 1. 配置样例

```
{
	"job": {
		"setting": {},
		"content": [{
			"reader": {
				"name": "esreader",
				"parameter": {
					"address": "host1:9200,host2:9200",
					"query": {
						"match": {
							"match_all": {}
						}
					},
					"index": "indexTest",
					"type": "type1",
					"batchSize": 0,
					"timeout": 10,
					"column": [{
						"name": "xx.yy.zz",
						"type": "string",
						"value": "value"
					}]
				}
			},
			"writer": {}
		}]
	}
}
```

## 2. 参数说明

* **address**
  
  * 描述：Elasticsearch地址，单个节点地址采用host:port形式，多个节点的地址用逗号连接
  
  * 必选：是
  
  * 默认值：无 

* **query**
  
  * 描述：Elasticsearch查询表达式，[查询表达式](https://www.elastic.co/guide/cn/elasticsearch/guide/current/query-dsl-intro.html) 
  
  * 必选：否 
  
  * 默认值：无，默认为全查询

* **batchSize**
  
  * 描述：每次读取数据条数
  
  * 必选：否
  
  * 默认值：10

* **timeout**
  
  * 描述：连接超时时间
  
  * 必选：否
  
  * 默认值：无

* **index**
  
  * 描述：要查询的索引名称
  
  * 必选：否
  
  * 默认值：无

* **type**
  
  * 描述：要查询的类型
  
  * 必选：否
  
  * 默认值：无

* **column**
  
  * 描述：读取elasticsearch的查询结果的若干个列，每列形式如下
    
    * name：字段名称，可使用多级格式查找
    
    * type：字段类型，当name没有指定时，则返回常量列，值为value指定
    
    * value：常量列的值
  
  * 必选：是
  
  * 默认值：无
