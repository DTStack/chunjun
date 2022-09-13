# S3 Reader



## 一、插件名称

名称：**s3reader**



## 二、支持的数据源版本

aws s3 所有版本



## 三、参数说明

#### 参数说明

- **accessKey**
    - 描述：aws 用户凭证：aws_access_key_id
    - 必选：是
    - 默认值：无



- **secretKey**
    - 描述：aws 用户凭证：aws_secret_access_key
    - 必选：是
    - 默认值：无



- **endpoint**
    - 描述：若需指定endpoint，则可通过该参数制定，详情可参见官方文档
      [https://docs.aws.amazon.com/zh_cn/general/latest/gr/rande.html](https://docs.aws.amazon.com/zh_cn/general/latest/gr/rande.html)
    - 必选：否
    - 默认值：根据 region 自动选择 endpoint



- **region**
    - 描述：储存桶的区域
    - 必选：否
    - 默认值：`us-west-2`



- **bucket**
    - 描述：存储桶名称
    - 必选：是
    - 默认值：无



- **objects**
    - 描述：需要同步的对象
    - 格式：
        - 单个对象
            - ["abc.xml"]
            - ["abd"]
        - 多个对象 必须以.*结尾代表读取此目录下所有文件,只支持目录名称前缀匹配，不支持多层级目录前缀匹配
           - ["as.*"] 会匹配as1，as2，as3目录下的所有文件
           - ["as/.*"] as 目录下所有文件



- **column**
    - 描述：需要读取的字段。
    - 格式：支持2种格式
        - 只指定字段名称：

        ```json
         "column":["id","name"]
        ```
    - 指定具体信息：
         - 属性说明
            - name：字段名称
            - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
            - format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
            - value：如果数据库里不存在指定的字段，则会把value的值作为常量列返回，如果指定的字段存在，当指定字段的值为null时，会以此value值作为默认值返回
         ```json
        "column": [{
            "index": 0,
            "type": "datetime",
            "format": "yyyy-MM-dd hh:mm:ss",
            "value": "value"
      }]
         ```
    - 必选：是
    - 默认值：无



- **encoding**
    - 描述：读取文件的编码配置
    - 必选：否
    - 默认值：`UTF-8`



- **fieldDelimiter**
    - 描述：读取的字段分隔符,只支持单字符或转义字符
    - 必选：否
    - 默认值：`,`



- **isFirstLineHeader**
    - 描述：首行是否为标题行，如果是则不读取第一行
    - 必选：否
    - 默认值：false


- **fetchSize**
    - 描述：单次请求获取目录下文件的数量
    - 必选：否
    - 默认值：512

- **useV2**
    - 描述：获取目录下文件数量的api
    - 必选：否
    - 默认值：true 使用ListObjectsV2Request，false使用 ListObjectsRequest

- **safetySwitch**
    - 描述：在文件编码等解析设置最终与文件的实际格式不匹配的情况下，防止解析器使用大量内存的安全注意事项。关闭后，解析器支持的每条记录的最大列长度和最大列数将大大增加。
    - 必选：否
    - 默认值：false(关闭)



**chunjun1.12 目前只支持string类型**

## 四、使用示例



#### 1、读取单个文件

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "accessKey": "",
            "secretKey": "",
            "endpoint": "http://127.0.0.1:9090",
            "region": "",
            "bucket": "",
            "objects": ["aaa.xml"],
            "column": [
              {
                "index": 0,
                "type": "string"
              },
              {
                "index": 1,
                "type": "string"
              },
              {
                "index": 2,
                "type": "int"
              },
              {
                "index": 3,
                "type": "int"
              }
            ],
            "encoding": "",
            "fieldDelimiter": ",",
            "isFirstLineHeader": true
          },
          "name": "s3reader"
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
      "log": {
        "isLogger": false,
        "level": "debug",
        "path": "",
        "pattern": ""
      }
    }
  }
}
```



#### 2、读取多个有文件

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "accessKey": "",
            "secretKey": "",
            "region": "",
            "bucket": "",
            "objects": [
              "aaa.xml",
              "bbb/ccc.xml"
            ],
            "column": [
              {
                "index": 0,
                "type": "string"
              },
              {
                "index": 1,
                "type": "string"
              },
              {
                "index": 2,
                "type": "int"
              },
              {
                "index": 3,
                "type": "int"
              }
            ],
            "encoding": "",
            "fieldDelimiter": ""
          },
          "name": "s3reader"
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
      "log": {
        "isLogger": false,
        "level": "debug",
        "path": "",
        "pattern": ""
      }
    }
  }
}
```



#### 3、读取多个路径下的多个文件

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "accessKey": "",
            "secretKey": "",
            "region": "",
            "bucket": "",
            "objects": ["dir/.+\.xml","bbb/ccc.xml"],
            "column": [
              {
                "index": 0,
                "type": "string"
              },
              {
                "index": 1,
                "type": "string"
              },
              {
                "index": 2,
                "type": "int"
              },
              {
                "index": 3,
                "type": "int"
              }
            ],
            "encoding": "",
            "fieldDelimiter": ""
          },
          "name": "s3reader"
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
      "log": {
        "isLogger": false,
        "level": "debug",
        "path": "",
        "pattern": ""
      }
    }
  }
}
```



#### 4、断点续传

开启断点续传功能需要给 column 中的每个字段都加上 name，name 值可以为不重复的任意字符串。并配置 restore，

restore 中 restoreColumnIndex 的值需选择 reader 中任意一个字段的 index，而 restoreColumnName 选取相应

字段的 name 的值。实际上这几个字段只是为了开启断点续传的功能用的，内部是使用文件流的偏移量进行恢复的。

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "accessKey": "test",
            "secretKey": "test",
            "endpoint": "http://127.0.0.1:9090",
            "region": "us-west-2",
            "bucket": "test",
            "objects": ["people_20210426001.csv"],
            "column": [
              {
                "name": "id",
                "index": 0,
                "type": "int"
              },
              {
                "name": "value1",
                "index": 1,
                "type": "string"
              },
              {
                "name": "value2",
                "index": 2,
                "type": "string"
              }
            ],
            "encoding": "UTF-8",
            "fieldDelimiter": ",",
            "isFirstLineHeader": true
          },
          "name": "s3reader"
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "username": "",
            "password": "",
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://localhost:3306/abc?useSSL=false",
                "table": [
                  "people_bak_20210421"
                ]
              }
            ],
            "postSql": [],
            "batchSize": 100,
            "writeMode": "insert",
            "column": [
              {
                "name": "id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "varchar"
              },
              {
                "name": "uuid",
                "type": "varchar"
              }
            ]
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
        "maxRowNumForCheckpoint": 100,
        "isStream" : true,
        "isRestore": true,
        "restoreColumnName" : "id",
        "restoreColumnIndex" : 1
      },
      "log": {
        "isLogger": false,
        "level": "debug",
        "path": "",
        "pattern": ""
      }
    }
  }
}
```
