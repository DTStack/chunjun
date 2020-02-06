# 分库分表读取插件（**dreader）

## 1. 配置样例

```
{
    "job": {
        "setting": {},
        "content": [
            {
              "reader": {
                "parameter": {
                  "password": "abc123",
                  "username": "dtstack",
                  "column": [
                    "col1",
                    "col2"
                  ],
                  "where": "id > 1",
                  "connection": [
                    {
                      "password": "abc123",
                      "username": "dtstack",
                      "jdbcUrl": [
                        "jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf8"
                      ],
                      "table": [
                        "tb2"
                      ]
                    }
                  ],
                  "splitPk": "id"
                },
                "name": "mysqldreader"
              },
               "writer": {}
            }
        ]
    }
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名，目前只支持mysql的分库分表读取，mysqldreader。
  
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
  
  * 描述：针对关系型数据库的jdbc连接字符串
  
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
  
  * 描述：筛选条件，MysqldReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。where条件可以有效地进行业务增量同步。如果不填写where语句，包括不提供where的key或者value，FlinkX均视作同步全量数据
  
  * 必选：否 <br />
  
  * 默认值：无 <br />

* **splitPk**
  
  * 描述：当speed配置中的channel大于1时指定此参数，Reader插件根据并发数和此参数指定的字段拼接sql，使每个并发读取不同的数据，提升读取速率。
  
  * 注意: 
    
    * 推荐splitPk使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。
    * 目前splitPk仅支持整形数据切分，`不支持浮点、字符串、日期等其他类型`。如果用户指定其他非支持类型，MysqlReader将报错！
    * 如果channel大于1但是没有配置此参数，任务将置为失败。
  
  * 必选：否
  
  * 默认值：空

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
