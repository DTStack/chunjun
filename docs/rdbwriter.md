# MySQL写入插件（*writer）

## 1. 配置样例

```
{
	"job": {
		"content": [{
			"reader": {},
			"writer": {
				"name": "*writer",

				"parameter": {
					"connection": [{
						"jdbcUrl": "jdbc:mysql://127.0.0.1:3306/test?useCursorFetch=true",
						"table": [
							"tableTest"
						]
					}],
					"username": "username",
					"password": "password",
					"column": [],

					"writeMode": "insert",
					"batchSize": 1024,
					"preSql": "",
					"postSql": "",
					"updateKey": ""
				}
			}
		}]
	},
	"setting": {}
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名，此处可填写：mysqlwriter，oraclewriter，sqlserverwriter，postgresqlwriter，db2writer
  * 必选：是
    
    默认值：无

* **jdbcUrl**
  
  * 描述：针对关系型数据库的jdbc连接字符串
  
  * 必选：是 
  
  * 默认值：无 

* **username**
  
  * 描述：数据源的用户名
  
  * 必选：是 
  
  * 默认值：无

* **password**
  
  * 描述：数据源指定用户名的密码
  
  * 必选：是 
  
  * 默认值：无

* **column**
  
  * 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。
  
  * 必选：是
  
  * 默认值：否 
  
  * 默认值：无 

* **preSql**
  
  * 描述：写入数据到目的表前，会先执行这里的一组标准语句。
  
  * 必选：否 
  
  * 默认值：无 

* **postSql**
  
  * 描述：写入数据到目的表后，会执行这里的一组标准语句。
  
  * 必选：否 
  
  * 默认值：无 

* **table**
  
  * 描述：目的表的表名称。目前只支持配置单个表，后续会支持多表。
  
  * 必选：是 
  
  * 默认值：无 

* **writeMode**
  
  * 描述：控制写入数据到目标表采用 `insert into` 或者 `replace into` 或者 `ON DUPLICATE KEY UPDATE` 语句
  
  * 必选：是 
  
  * 所有选项：insert/replace/update 
  
  * 默认值：insert 

* **batchSize**
  
  * 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与数据库的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况。
  
  * 必选：否
  
  * 默认值：1024 

* **updateKey**
  
  * 描述：当写入模式为update和replace时，需要指定此参数的值为唯一索引字段。
  
  * 注意：
    
    * 如果此参数为空，并且写入模式为update和replace时，应用会自动获取数据库中的唯一索引；
    
    * 如果数据表没有唯一索引，但是写入模式配置为update和replace，应用会以insert的方式写入数据；
  
  * 必选：否
  
  * 默认值：无
