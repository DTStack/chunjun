# Elasticsearch写入插件（eswriter）

## 1. 配置样例

```
{
	"job": {
		"setting": {},
		"content": [{
			"reader": {},
			"writer": {
				"name": "eswriter",
				"parameter": {
					"address": "host1:9200,host2:9200",
					"index": "indexTest",
					"type": "type1",
					"bulkAction": 100,
					"timeout": 100,
					"idColumn": [{
						"index": 0,
						"type": "int"
					}],
					"column": [{
						"name": "col1",
						"type": "string"
					}]
				}
			}
		}]
	}
}
```

## 2. 参数说明

* **address**
  
  * 描述：Elasticsearch地址，单个节点地址采用host:port形式，多个节点的地址用逗号连接 
  
  * 必选：是 
  
  * 默认值：无

* **index**
  
  * 描述：Elasticsearch 索引值 
  
  * 必选：是 
  
  * 默认值：无 

* **type**
  
  * 描述：Elasticsearch 索引类型
  
  * 必选：是 
  
  * 默认值：无 

* **column**
  
  * 描述：写入elasticsearch的若干个列，每列形式如下
    
    ```
      {
          "name": "列名",
          "type": "列类型"
      }
    ```
  
  * 必选：是
  
  * 默认值：无

* **idColumns**
  
  * 描述：用于构造文档id的若干个列，每列形式如下
    
    * 普通列
      
      ```
      {
        "index": 0,  // 前面column属性中列的序号，从0开始
        "type": "string" 列的类型，默认为string
      }
      ```
    
    * 常数列
      
      ```
      {
        "value": "ffff", // 常数值
        "type": "string" // 常数列的类型，默认为string
      }
      ```
  
  * 必选：否 
  
  * 注意：
    
    * 如果不指定idColumns属性，则会随机产生文档id
    
    * 如果指定的字段值存在重复或者指定了常数，按照es的逻辑，同样值的doc只会保留一份
  
  * 默认值：无

* **bulkAction**
  
  * 描述：批量写入的记录条数
  
  * 必选：是 
  
  * 默认值：100 

* **timeout**
  
  * 描述：连接超时时间，如果bulkAction指定的数值过大，写入数据可能会超时，这时可以配置超时时间
  
  * 必选：否
  
  * 默认值：无
