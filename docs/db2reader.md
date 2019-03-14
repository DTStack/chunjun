# MySQL分库分表读取插件（db2reader）

## 1. 配置样例

```
{
  "job": {
    "content": [
      {
        "reader": {
          "fetchSize": 1024,
          "parameter": {
            "password": "abc123",
            "column": [
              "smallint_col",
              "integer_col",
              "bigint_col",
              "decimal_col",
              "real_col",
              "double_col"
            ],
            "where": "",
            "connection": [
              {
                "password": "abc123",
                "jdbcUrl": ["jdbc:db2://172.16.1.191:50000/flinkx"],
                "table": [
                  "db2_stand_all"
                ],
                "username": "dtstack"
              }
            ],
            "splitPk": "",
            "username": "dtstack"
          },
          "name": "db2reader"
        },
        "writer": {
          "parameter": {
            "print":true
          },
          "name": "streamwriter"
        }
      }
    ],
    "setting": {
      "errorLimit": {
      },
      "speed": {
        "bytes": 0,
        "channel": 1
      }
    }
  }
}

```

## 2. 参数说明

* **name**

 	* 描述：插件名，此处只能填db2reader，否则Flinkx将无法正常加载该插件包。
 		
	* 必选：是 <br />

	* 默认值：无 <br />

* **connection**

    * 描述：需要读取的数据源数组。
    
    * 必选：是
    
    * 默认值：无
    
    * 元素：
    
        * username：具体数据源的用户名，如果不填则使用全局的用户名。
        
        * password：具体数据源的密码，如果不填则使用全局的密码。
        
        * jdbcUrl：数据源连接url，只支持写单个连接。
        
        * table：要查询的表名称，可写多张表，多张表的表结构必须一致。

* **jdbcUrl**

	* 描述：针对db2数据库的jdbc连接字符串

		jdbcUrl按照DB2官方规范，并可以填写连接附件控制信息。具体请参看[DB2官方文档](https://www.ibm.com/analytics/us/en/db2/)。

	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：全局数据源的用户名 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **password**

	* 描述：全局数据源的密码 <br />

	* 必选：否 <br />

	* 默认值：无 <br />
	
* **where**

	* 描述：筛选条件，MysqldReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

          where条件可以有效地进行业务增量同步。如果不填写where语句，包括不提供where的key或者value，FlinkX均视作同步全量数据。

	* 必选：否 <br />

	* 默认值：无 <br />
	
* **splitPk**

	* 描述：MysqldReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，FlinkX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

	  推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

	  目前splitPk仅支持整形数据切分，`不支持浮点、字符串、日期等其他类型`。如果用户指定其他非支持类型，MysqldReader将报错！

	  如果splitPk不填写，包括不提供splitPk或者splitPk值为空，FlinkX视作使用单通道同步该表数据。

	* 必选：否 <br />

	* 默认值：空 <br />



* **column**

	* 描述：所配置的表中需要同步的列名集合。
	
	  支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。
      
      暂不支持常量列。

	* 必选：是 <br />

	* 默认值：无 <br />
	
