# PostgreSQL写入插件（postgresqlwriter）

## 1. 配置样例
```json
{
	"job":{
		"content":[{
			"reader":{},
			"writer":{
				"parameter":{
					"postSql": [],
					"password": "postgres",
					"session": [],
					"column": ["id", "data_name"],
					"connection": [{
						"jdbcUrl": "jdbc:postgresql://localhost:5432/postgres",
						"table": ["table1"]
					}],
					"writeMode": "insert",
					"preSql": [],
					"username": "postgres"
				},
				"name":"postgresqlwriter"
			}
		}]
	}
}
```


## 2. 参数说明

* **name**

 	* 描述：插件名，此处只能填postgresqlwriter，否则Flinkx将无法正常加载该插件包。
	* 必选：是 <br />

	* 默认值：无 <br />

* **jdbcUrl**

	* 描述：针对PostgreSQL数据库的jdbc连接字符串

		jdbcUrl按照PostgreSQL官方规范，并可以填写连接附件控制信息。具体请参看[PostgreSQL官方文档](https://jdbc.postgresql.org/documentation/head/connect.html)。

	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：数据源的用户名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：数据源指定用户名的密码 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。
	
	* 必选：是 <br />

	* 默认值：否 <br />

	* 默认值：无 <br />

* **preSql**

	* 描述：写入数据到目的表前，会先执行这里的一组标准语句。
	
	* 必选：否 <br />

	* 默认值：无 <br />

* **postSql**

	* 描述：写入数据到目的表后，会执行这里的一组标准语句。

	* 必选：否 <br />

	* 默认值：无 <br />

* **table**

	* 描述：目的表的表名称。目前只支持配置单个表，后续会支持多表。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

	* 必选：是 <br />

	* 默认值：无 <br />

* **writeMode**

	* 描述：控制写入数据到目标表采用 `insert into` 或者 `insert into .... on conflict(id) do update set ..` 语句。<br />
	        
	        注：PostgreSQL 9.5之前的版本不支持`insert into .... on conflict(id) do update set ..`语法，因此如果PostgreSQL的版本小于9.5，PostgreSQLWriter插件的update和replace模式将无法使用

	* 必选：是 <br />
	
	* 所有选项：insert/replace/update <br />

	* 默认值：insert <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与PostgreSQL的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />