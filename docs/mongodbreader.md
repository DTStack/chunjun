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
                    "fetchSize":100,
                    "column": [
                        {
                            "name":"id",
                            "type":"int",
                            "splitter":","
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
        "value": "value",
        "splitter":","
    }]
    ```
  
  * 属性说明:
    
    * name：字段名称
    
    * type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
    
    * format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
    
    * value：如果数据库里不存在指定的字段，则会把value的值作为常量列返回，如果指定的字段存在，当指定字段的值为null时，会以此value值作为默认值返回
    
    * splitter：因为 MongoDB 支持数组类型，所以 MongoDB 读出来的数组类型要通过这个分隔符合并成字符串
  
  * 必选：是
  
  * 默认值：无

* **fetchSize**
  
  * 描述：每次读取的数据条数，通过调整此参数来优化读取速率
  
  * 必选：否
  
  * 默认值：100

* **filter**
  
  * 描述：过滤条件，通过该配置型来限制返回 MongoDB 数据范围，语法请参考[MongoDB查询语法](https://docs.mongodb.com/manual/crud/#read-operations)
  
  * 必选：否
  
  * 默认值：无
