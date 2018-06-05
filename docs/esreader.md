# Elasticsearch读取插件（esreader）

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
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "esreader",
                    "parameter": {
                        "address": "rdos1:9200,rdos2:9200",
                        "query": {
                          "match": {
                            "col2": "hallo"
                          }
                        },
                        "column": [
                          {
                            "name": "xx.yy.zz",
                            "type": "string"
                          },
                          {
                            "name": "col2",
                            "type": "string"
                          }
                        ]
                    }
                },
               "writer": {
                    "name": "mysqlwriter",
                    "parameter": {
                        "writeMode": "insert",
                        "username": "dtstack",
                        "password": "abc123",
                        "column": [
                            "col1",
                            "col2"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://172.16.8.104:3306/test?useCursorFetch=true",
                                "table": [
                                    "tb333"
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

* **address**

	* 描述：Elasticsearch地址，单个节点地址采用host:port形式，多个节点的地址用逗号连接 <br />

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **query**

	* 描述：Elasticsearch查询表达式，[查询表达式](https://www.elastic.co/guide/cn/elasticsearch/guide/current/query-dsl-intro.html)  <br />

	* 必选：否 <br />

	* 默认值：无，默认为全查询<br />
	
* **column**

	* 描述：读取elasticsearch的查询结果的若干个列，每列形式如下<br />
		* 普通列
	
		```
		{
			"name": "xx.yy.zz", //支持列的多级嵌套，用.连接
			"type": "string"
		}
		```
		* 常数列
		
		```
		{
			"value": "xxx", // 常量值
			"type": "string" //常量类型
		}
		```

	* 必选：是 <br />

	* 默认值：无 <br />
