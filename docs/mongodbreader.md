# MongoDB读取插件（mongodbreader）

## 1. 配置样例
```json
{
	"job":{
		"content":[{
			"reader":{
				"parameter":{
					"hostPorts":"localhost:27017",
					"username": "",
					"password": "",
					"database":"",
					"collectionName": "",
					"column": [
						{
							"name":"id",
							"type":"int",
							"splitter":","
						},
						{
							"name":"id",
							"type":"string"
						}
					],
					"filter": ""
				},
				"name":"mongodbreader"
			},
			"writer":{}
		}]
	}
}
```

## 2. 参数说明

* **name**

 	* 描述：插件名，此处只能填mongodbreader，否则Flinkx将无法正常加载该插件包。
 		
	* 必选：是

	* 默认值：无

* **hostPorts**

	* 描述：MongoDB的地址和端口，格式为 IP1:port，可填写多个地址，以英文逗号分隔。

	* 必选：是

	* 默认值：无

* **username**

	* 描述：数据源的用户名

	* 必选：否

	* 默认值：无

* **password**

	* 描述：数据源指定用户名的密码

	* 必选：否

	* 默认值：无
	
* **database**

	* 描述：数据库名称

	* 必选：是

	* 默认值：无
	
* **collectionName**

	* 描述：集合名称

	* 必选：是

	* 默认值：无

* **column**

	* 描述：MongoDB 的文档列名，配置为数组形式表示 MongoDB 的多个列。
         - name：Column 的名字。
         - type：Column 的类型。
         - splitter：因为 MongoDB 支持数组类型，所以 MongoDB 读出来的数组类型要通过这个分隔符合并成字符串。
	
	* 必选：是

	* 默认值：无
	
* **filter**

	* 描述：过滤条件，通过该配置型来限制返回 MongoDB 数据范围，语法请参考[MongoDB查询语法](https://docs.mongodb.com/manual/crud/#read-operations)

	* 必选：否

	* 默认值：无