# Elasticsearch写入插件（eswriter）

## 1. 配置样例

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3,
        "bytes": 10000000
      },
      "errorLimit": {
        "record": 0,
        "percentage": 20
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
                "col1",
                "col2"
            ],
            "splitPk": "col1",
            "connection": [
                {
                    "table": [
                        "tb2"
                    ],
                    "jdbcUrl": [
                        "jdbc:mysql://172.16.8.104:3306/test?useCursorFetch=true"
                    ]
                }
            ]
          }
        },
        "writer": {
          "name": "eswriter",
          "parameter": {
            "address": "rdos1:9200,rdos2:9200",
            "index": "yoshi",
            "type": "nani",
            "bulkAction": 3,
            "idColumn": [
              {
                "index": 0,
                "type": "int"
              }
            ],
            "column": [
              {
                "name": "col1",
                "type": "string"
              },
              {
                "name": "col2",
                "type": "string"
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

* **address**

	* 描述：Elasticsearch地址，单个节点地址采用host:port形式，多个节点的地址用逗号连接 <br />

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **index**

	* 描述：Elasticsearch 索引值 <br />

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **type**

	* 描述：Elasticsearch 索引类型<br />

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **column**

	* 描述：写入elasticsearch的若干个列，每列形式如下<br />
	
		```
		{
			"name": "列名",
			"type": "列类型"
		}
		```

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **idColumns**

	* 描述：用于构造文档id的若干个列，每列形式如下<br />
	
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
		
	* 必选：否 <br />
		如果不指定idColumns属性，则会随机产生文档id

	* 默认值：无 <br />
	
	
* **bulkAction**

	* 描述：批量写入的记录条数 <br />

	* 必选：是 <br />

	* 默认值：100 <br />