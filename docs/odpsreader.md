# ODPS读取插件（odpsreader）

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
        "percentage": 0.02
      }
    },
    "content": [
      {
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "writeMode": "insert",
            "username": "dtstack",
            "password": "abc123",
            "column": [
              "c1",
              "c2"
            ],
            "batchSize": 1,
            "session": [
              "set session sql_mode='ANSI'"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://172.16.8.104:3306/test?useCursorFetch=true",
                "table": [
                  "tb3"
                ]
              }
            ]
          }
        },
        "reader": {
          "name": "odpsreader",
          "parameter": {
            "odpsConfig": {
              "accessId": "${odps.accessId}",
              "accessKey": "${odps.accessKey}",
              "project": "${odps.project}"
            },
            "table": "tb252",
            "partition": "pt='xxooxx'",
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


* **accessId**
	* 描述：ODPS系统登录ID <br />

 	* 必选：是 <br />

 	* 默认值：无 <br />

* **accessKey**
	* 描述：ODPS系统登录Key <br />

 	* 必选：是 <br />

 	* 默认值：无 <br />

* **project**

	* 描述：读取数据表所在的 ODPS 项目名称（大小写不敏感） <br />

	* 必选：是 <br />

 	* 默认值：无 <br />

* **table**

 	* 描述：读取数据表的表名称（大小写不敏感） <br />

 	* 必选：是 <br />

 	* 默认值：无 <br />
 	
* **partition**

	* 描述：读取数据所在的分区信息，支持linux shell通配符，包括 * 表示0个或多个字符，?代表任意一个字符。例如现在有分区表 test，其存在 pt=1,ds=hangzhou   pt=1,ds=shanghai   pt=2,ds=hangzhou   pt=2,ds=beijing 四个分区，如果你想读取 pt=1,ds=shanghai 这个分区的数据，那么你应该配置为: `"partition":["pt=1,ds=shanghai"]`； 如果你想读取 pt=1下的所有分区，那么你应该配置为: `"partition":["pt=1,ds=* "]`；如果你想读取整个 test 表的所有分区的数据，那么你应该配置为: `"partition":["pt=*,ds=*"]` <br />

	* 必选：如果表为分区表，则必填。如果表为非分区表，则不能填写 <br />

	* 默认值：无 <br />
	
* **column**

	* 描述：读取 odps 源头表的列信息，包括需要选取的列，每列的格式如下：
		* 根据字段名指定列

		```
{
  "name": 'col1'    //获取字段名为col1的字段
}
		```

		* 指定常量列

		```
{
  "type": "string",
  "value": "yesyoucan"  //OdpsReader内部生成yesyoucan的字符串字段作为当前字段
}
		```


