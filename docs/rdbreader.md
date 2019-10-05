# 关系数据库读取插件（*reader）

## 1. 配置样例

```
{
    "job": {
        "content": [{
            "reader": {
                "parameter": {
                    "username": "username",
                    "password": "password",
                    "connection": [{
                        "jdbcUrl": [
                            "jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf8"
                        ],
                        "table": [
                            "tableTest"
                        ]
                    }],
                    "column": [{
                        "name": "id",
                        "type": "int",
                        "values": 123
                    },{
                        "name":"",
                        "index":1,
                        "type":"",
                        "value":"",
                        "format":""
                    }],
                    "where": "id > 1",
                    "splitPk": "id",
                    "fetchSize": 1000,
                    "queryTimeOut": 1000,
                    "customSql": "select * from tableTest",
                    "requestAccumulatorInterval": 2,
                    "increColumn": "id",
                    "startLocation": null,
                    "useMaxFunc": true,
                    "orderByColumn": "id"
                },
                "name": "mysqlreader"
            },
            "writer": {

            }
        }]
    },
    "setting": {

    }
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名，此处填写插件名称，当前支持的关系数据库插件包括：mysqlreader，oraclereader，sqlserverreader，postgresqlreader，db2reader，gbasereader。    
    * 必选：是 
    
    * 默认值：无 

* **jdbcUrl**
  
  * 描述：针对关系型数据库的jdbc连接字符串
    
      jdbcUrl参考文档：
    
    - [Mysql官方文档](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)
    
    - [Oracle官方文档](http://www.oracle.com/technetwork/database/enterprise-edition/documentation/index.html)
    
    - [SqlServer官方文档](https://docs.microsoft.com/zh-cn/sql/connect/jdbc/overview-of-the-jdbc-driver?view=sql-server-2017)
    
    - [PostgreSql官方文档](https://jdbc.postgresql.org/documentation/head/connect.html)
    
    - [Db2官方文档](https://www.ibm.com/analytics/us/en/db2/)
    
    - [Gbase官方文档](http://www.gbase.cn/download.html)
  
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

* **where**
  
  * 描述：筛选条件，reader插件根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > time。
  
  * 注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。
  
  * 必选：否
  
  * 默认值：无

* **splitPk**
  
  * 描述：当speed配置中的channel大于1时指定此参数，Reader插件根据并发数和此参数指定的字段拼接sql，使每个并发读取不同的数据，提升读取速率。
  
  * 注意: 
    
    * 推荐splitPk使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。
    * 目前splitPk仅支持整形数据切分，`不支持浮点、字符串、日期等其他类型`。如果用户指定其他非支持类型，MysqlReader将报错！
    * 如果channel大于1但是没有配置此参数，任务将置为失败。
  
  * 必选：否
  
  * 默认值：空

* **fetchSize**
  
  * 描述：读取时每批次读取的数据条数。
  
  * 注意：此参数的值不可设置过大，否则会读取超时，导致任务失败。
  
  * 必选：否
  
  * 默认值：mysql为0，表示流式读取，其它数据库为1000

* **queryTimeOut**
  
  * 描述：查询超时时间，单位秒。
  
  * 注意：当数据量很大，或者从视图查询，或者自定义sql查询时，可通过此参数指定超时时间。
  
  * 必选：否
  
  * 默认值：1000s

* **customSql**
  
  * 描述：自定义的查询语句，如果只指定字段不能满足需求时，可通过此参数指定查询的sql，可以是任意复杂的查询语句。
  
  * 注意：
    
    * 只能是查询语句，否则会导致任务失败；
    
    * 查询语句返回的字段需要和column列表里的字段对应；
    
    * 当指定了此参数时，connection里指定的table无效，但是在一些情况下依然必须指定，比如使用增量同步的时候；
  
  * 必选：否
  
  * 默认值：null

* **increColumn**
  
  * 描述：当需要增量同步时指定此参数，任务运行过程中会把此字段的值存储到flink的Accumulator里，如果配置了指标，名称为：endLocation，类型为string，日期类型会转为时间戳，精度最多到纳秒，数值类型的为字段的值，程序结束时由外部应用获取。
  
  * 注意：
    
    * 指定的字段必须在column列表里存在，否则任务会失败；
    
    * 增量字段支持数值类型和日期类型，并且是升序的，推荐使用表主键；
  
  * 必选：否
  
  * 默认值：无

* **startLocation**
  
  * 描述：此配置参数和increColumn参数配合使用，表示本次任务获取数据的开始位置。
  
  * 注意：
    
    * 此参数为空时进行全量同步
  
  * 必选：否
  
  * 默认值：无

* **useMaxFunc**
  
  * 描述：进行增量同步任务时，如果指定的字段值存在重复值，比如字段类型为时间，精度到秒，就可能出现重复的时间，需要指定此字段为true，读取数据前会获取增量字段的最大值作为此次任务的结束位置，防止数据丢失。
  
  * 注意：
    
    * 此参数设为true时，会执行select max(increCol) from tb语句，会影响数据库负载，配置时需要考虑数据库的使用情况；
    
    * 此参数设置为true时，本次任务不会读取 increCol = max(increCol)的记录，会在任务下次运行时读取；
  
  * 必选：否
  
  * 默认：false

* **column**
  
  * 描述：需要读取的字段。
  
  * 格式：支持3中格式
    
    1.读取全部字段，如果字段数量很多，可以使用下面的写法：
    
    ```
    "column":[*]
    ```
    
    2.只指定字段名称：
    
    ```
    "column":["id","name"]
    ```
    
    3.指定具体信息：
    
    ```
    "column": [{
        "name": "col",
        "type": "datetime",
        "format": "yyyy-MM-dd hh:mm:ss",
        "value": "value"
    }]
    ```
  
  * 属性说明:
    
    * name：字段名称
    
    * type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
    
    * format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
    
    * value：如果数据库里不存在指定的字段，则会把value的值作为常量列返回，如果指定的字段存在，当指定字段的值为null时，会以此value值作为默认值返回
  
  * 必选：是
  
  * 默认值：无

* **orderByColumn**
  
  * 描述：排序字段，读取PostgreSQL数据时，如果中途任务失败，没有关闭事务，会导致表里的数据顺序改变，再次运行任务时由于数据顺序不对会影响数据的准确性，因此使用orderByColumn指定的字段进行排序避免这种情况。
  
  * 必选：否
  
  * 默认值：无
