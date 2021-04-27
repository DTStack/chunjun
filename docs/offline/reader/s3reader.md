# FTP Reader

<a name="lq07M"></a>
## 一、插件名称
名称：**s3reader**<br />

<a name="AsTTs"></a>
## 二、支持的数据源版本
xxx


<a name="j2xad"></a>
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



- **region**
  - 描述：储存桶的区域
  - 必选：否
  - 默认值：`us-west-2`



- **bucket**
  - 描述：存储桶名称
  - 必选：是
  - 默认值：无

- **object**
  - 描述：需要同步的对象
  - 格式：
    - 单个对象
      - [abc.xml]
      - [abd]
    - 多个对象
      - [dir\\*]
      - [abc.xml,dir\\*]

- **column**

  - 描述：需要读取的字段。

  - 格式：支持3种格式

    - 读取全部字段，如果字段数量很多，可以使用下面的写法：

    ```json
    "column":["*"]		
    ```

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
  - 描述：读取的字段分隔符
  - 必选：是
  - 默认值：`,`
- **isFirstLineHeader**
  - 描述：首行是否为标题行，如果是则不读取第一行
  - 必选：否
  - 默认值：false


<a name="QQaDC"></a>
## 四、使用示例
<a name="1nZ3r"></a>
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
            "region": "",
            "bucket": "",
            "object": ["aaa.xml"],
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
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false
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
<a name="BTnag"></a>
#### 2、读取多个有文件
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "p
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
                  "object": ["aaa.xml","bbb/ccc.xml"],
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
            "restore": {
              "maxRowNumForCheckpoint": 0,
              "isRestore": false
            },
            "log": {
              "isLogger": false,
              "level": "debug",
              "path": "",
              "pattern": ""
            }
          }
        }
      }arameter": {
            "accessKey": "",
            "secretKey": "",
            "region": "",
            "bucket": "",
            "object": ["aaa.xml","bbb/ccc.xml"],
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
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false
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
<a name="KxOTY"></a>
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
            "object": ["aaa/*","bbb/ccc.xml"],
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
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false
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


