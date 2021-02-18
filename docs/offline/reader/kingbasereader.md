# KingBase Reader

## 一、插件名称
名称：**kingbasereader**
<br/>
## 二、支持的数据源版本
**KingBase 8.2、8.3**  

## 三、参数说明
- jdbcUrl
    - 描述：针对KingBase数据库的jdbc连接字符串
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br/>

- username
    - 描述：数据源的用户名
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br/>

- password
    - 描述：数据源指定用户名的密码
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br/> 

- schema
    - 描述：查询数据库所在schema
    - 必选：是
    - 字段类型：String
    - 默认值：无

<br/>

- where
    - 描述：筛选条件，reader插件根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > time。
    - 注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。
    - 必选：否
    - 字段类型：String
    - 默认值：无

<br/>

- splitPk
    - 描述：当speed配置中的channel大于1时指定此参数，Reader插件根据并发数和此参数指定的字段拼接sql，使每个并发读取不同的数据，提升读取速率。
    - 注意:  
    推荐splitPk使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。  
    目前splitPk仅支持整形数据切分，不支持浮点、字符串、日期等其他类型。如果用户指定其他非支持类型，FlinkX将报错！  
    如果channel大于1但是没有配置此参数，任务将置为失败。  
    - 必选：否
    - 字段类型：String
    - 默认值：无

<br/>

- queryTimeOut
    - 描述：查询超时时间，单位秒。
    - 注意：当数据量很大，或者从视图查询，或者自定义sql查询时，可通过此参数指定超时时间。
    - 必选：否
    - 字段类型：int
    - 默认值：1000
 
<br/>

- customSql
    - 描述：自定义的查询语句，如果只指定字段不能满足需求时，可通过此参数指定查询的sql，可以是任意复杂的查询语句。
    - 注意：  
    只能是查询语句，否则会导致任务失败；  
    查询语句返回的字段需要和column列表里的字段对应；  
    当指定了此参数时，connection里指定的table无效；  
    当指定此参数时，column必须指定具体字段信息，不能以*号代替；
    - 必选：否
    - 字段类型：String
    - 默认值：无
 
<br/>

- column
    - 描述：需要读取的字段。
    - 格式：支持3种格式  
    1.读取全部字段，如果字段数量很多，可以使用下面的写法：  
    "column":["*"]  
    2.只指定字段名称：  
    "column":["id","name"]  
    3.指定具体信息：
```json
    "column": [{
        "name": "col",
        "type": "datetime",
        "format": "yyyy-MM-dd hh:mm:ss",
        "value": "value"
    }]
```
   &emsp; 属性说明:  
   &emsp; name：字段名称  
   &emsp; type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换  
   &emsp; format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回  
   &emsp; value：如果数据库里不存在指定的字段，则会把value的值作为常量列返回，如果指定的字段存在，当指定字段的值为null时，会以此value值作为默认值返回  
   - 必选：是
   - 字段类型：List
   - 默认值：无
  
<br/>

- polling
    - 描述：是否开启间隔轮询，开启后会根据pollingInterval轮询间隔时间周期性的从数据库拉取数据。开启间隔轮询还需配置参数pollingInterval，increColumn，可以选择配置参数startLocation。若不配置参数startLocation，任务启动时将会从数据库中查询增量字段最大值作为轮询的开始位置。
    - 必选：否
    - 默认值：false
  
<br/>   
    
- pollingInterval
    - 描述：轮询间隔时间，从数据库中拉取数据的间隔时间，默认为5000毫秒。
    - 必选：否
    - 字段类型：int
    - 默认值：5000

<br/>

- requestAccumulatorInterval
    - 描述：发送查询累加器请求的间隔时间。
    - 必选：否
    - 字段类型：int
    - 默认值：2

<br/>
    
## 四、配置示例
1、基础配置
```
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "username": "dtstack",
            "password": "abc123",
            "connection": [{
              "jdbcUrl": ["jdbc:kingbase8://localhost:54321/test"],
              "table": ["kudu"],
              "schema":"test"
            }],
            "column": ["*"],
            "customSql": "",
            "where": "id < 100",
            "splitPk": "",
            "queryTimeOut": 1000,
            "requestAccumulatorInterval": 2
          },
          "name": "kingbasereader"
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "log" : {
        "isLogger": false,
        "level" : "debug",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```
2、多通道
```
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "username": "dtstack",
            "password": "abc123",
            "connection": [{
              "jdbcUrl": ["jdbc:kingbase8://localhost:54321/test"],
              "table": ["kudu"],
              "schema":"test"
            }],
            "column": ["*"],
            "customSql": "",
            "where": "id < 100",
            "splitPk": "id",
            "queryTimeOut": 1000,
            "requestAccumulatorInterval": 2
          },
          "name": "kingbasereader"
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 2,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "log" : {
        "isLogger": false,
        "level" : "debug",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```
3、指定customSql
```
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "username": "dtstack",
            "password": "abc123",
            "connection": [{
              "jdbcUrl": ["jdbc:kingbase8://localhost:54321/test"],
              "table": ["kudu"],
              "schema":"test"
            }],
            "column": ["id","user_id","name"],
            "customSql": "select * from kudu where id > 20",
            "where": "id < 100",
            "splitPk": "",
            "queryTimeOut": 1000,
            "requestAccumulatorInterval": 2
          },
          "name": "kingbasereader"
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "log" : {
        "isLogger": false,
        "level" : "debug",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```
4、增量同步指定startLocation
```
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "username": "dtstack",
            "password": "abc123",
            "connection": [{
              "jdbcUrl": ["jdbc:kingbase8://localhost:54321/test"],
              "table": ["kudu"],
              "schema":"test"
            }],
            "column": [{
              "name": "id",
              "type": "bigint"
            },{
              "name": "user_id",
              "type": "bigint"
            },{
              "name": "name",
              "type": "varchar"
            }],
            "customSql": "",
            "where": "id < 100",
            "splitPk": "id",
            "increColumn": "id",
            "startLocation": "20",
            "queryTimeOut": 1000,
            "requestAccumulatorInterval": 2
          },
          "name": "kingbasereader"
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": true,
        "restoreColumnName": "id",
        "restoreColumnIndex": 1
      },
      "log" : {
        "isLogger": false,
        "level" : "debug",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```
5、间隔轮询
```
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "username": "dtstack",
            "password": "abc123",
            "connection": [{
              "jdbcUrl": ["jdbc:kingbase8://localhost:54321/test"],
              "table": ["kudu"],
              "schema":"test"
            }],
            "column": [{
              "name": "id",
              "type": "bigint"
            },{
              "name": "user_id",
              "type": "bigint"
            },{
              "name": "name",
              "type": "varchar"
            }],
            "customSql": "",
            "where": "id > 100",
            "splitPk": "id",
            "increColumn": "id",
            "startLocation": "20",
            "polling": true,
            "pollingInterval": 3000,
            "queryTimeOut": 1000,
            "requestAccumulatorInterval": 2
          },
          "name": "kingbasereader"
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": 0
      },
      "errorLimit": {
        "record": 100
      },
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": true,
        "restoreColumnName": "id",
        "restoreColumnIndex": 1
      },
      "log" : {
        "isLogger": false,
        "level" : "debug",
        "path" : "",
        "pattern":""
      }
    }
  }
}
```