# FTP Reader

<a name="lq07M"></a>
## 一、插件名称
名称：**ftpreader**<br />

<a name="AsTTs"></a>
## 二、数据源版本
| 协议 | 是否支持 |
| --- | --- |
| FTP | 支持 |
| SFTP | 支持 |



<a name="VMxbP"></a>
## 三、数据源配置
FTP服务搭建<br />windows：[地址](https://help.aliyun.com/document_detail/92046.html?spm=a2c4g.11186623.6.1185.6371dcd5DOfc5z)<br />linux：[地址](https://help.aliyun.com/document_detail/92048.html?spm=a2c4g.11186623.6.1184.7a9a2dbcRLDNlf)<br />sftp服务搭建<br />windows：[地址](http://www.freesshd.com/)<br />linux：[地址](https://yq.aliyun.com/articles/435356?spm=a2c4e.11163080.searchblog.102.576f2ec1BVgWY7)<br />

<a name="j2xad"></a>
## 四、参数说明

- **protocol**
  - 描述：ftp服务器协议，目前支持传输协议有`ftp`、`sftp`
  - 必选：是
  - 默认值：无



- **host**
  - 描述：ftp服务器地址
  - 必选：是
  - 默认值：无



- **port**
  - 描述：ftp服务器端口
  - 必选：否
  - 默认值：若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21



- **connectPattern**
  - 描述：协议为ftp时的连接模式，可选`pasv`，`port`，参数含义可参考：[模式说明](https://blog.csdn.net/qq_16038125/article/details/72851142)
  - 必选：否
  - 默认值：`PASV`



- **username**
  - 描述：ftp服务器访问用户名
  - 必选：是
  - 默认值：无



- **password**
  - 描述：ftp服务器访问密码
  - 必选：否
  - 默认值：无



- **path**
  - 描述：远程FTP文件系统的路径信息，注意这里可以支持填写多个路径
  - 必选：是
  - 默认值：无



- **fieldDelimiter**
  - 描述：读取的字段分隔符
  - 必选：是
  - 默认值：`,`



- **encoding**
  - 描述：读取文件的编码配置
  - 必选：否
  - 默认值：`UTF-8`



- **isFirstLineHeader**
  - 描述：首行是否为标题行，如果是则不读取第一行
  - 必选：否
  - 默认值：false



- **timeout**
  - 描述：连接超时时间，单位毫秒
  - 必选：否
  - 默认值：5000



- **column**
  - 描述：需要读取的字段
  - 格式：支持2中格式
<br />1.读取全部字段，如果字段数量很多，可以使用下面的写法：
```
"column":["*"]
```

2.指定具体信息：
```
"column": [{
    "index": 0,
    "type": "datetime",
    "format": "yyyy-MM-dd hh:mm:ss",
    "value": "value"
}]
```

  - 属性说明:
    - index：字段索引
    - type：字段类型，ftp读取的为文本文件，本质上都是字符串类型，这里可以指定要转成的类型
    - format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
    - value：如果没有指定index，则会把value的值作为常量列返回，如果指定了index，当读取的字段的值为null时，会以此value值作为默认值返回
  - 必选：是
  - 默认值：无



<a name="QQaDC"></a>
## 五、使用示例
<a name="1nZ3r"></a>
#### 1、读取单个文件
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "parameter": {
                        "path": "/data/ftp/flinkx/file1.csv",
                        "protocol": "sftp",
                        "port": 22,
                        "isFirstLineHeader": true,
                        "host": "localhost",
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
                        "password": "pass",
                        "fieldDelimiter": ",",
                        "encoding": "utf-8",
                        "username": "user"
                    },
                    "name": "ftpreader"
                },
                "writer": {
                    "parameter": {},
                    "name": "streamwriter"
                }
            }
        ],
        "setting": {
            "restore": {
                "maxRowNumForCheckpoint": 0,
                "isRestore": false,
                "restoreColumnName": "",
                "restoreColumnIndex": 0
            },
            "errorLimit": {
                "record": 100
            },
            "speed": {
                "bytes": 0,
                "channel": 1
            }
        }
    }
}
```
<a name="BTnag"></a>
#### 2、读取单个目录下的所有文件
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "parameter": {
                        "path": "/data/ftp/flinkx/dir1",
                        "protocol": "sftp",
                        "port": 22,
                        "isFirstLineHeader": true,
                        "host": "localhost",
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
                        "password": "pass",
                        "fieldDelimiter": ",",
                        "encoding": "utf-8",
                        "username": "user"
                    },
                    "name": "ftpreader"
                },
                "writer": {
                    "parameter": {},
                    "name": "streamwriter"
                }
            }
        ],
        "setting": {
            "restore": {
                "maxRowNumForCheckpoint": 0,
                "isRestore": false,
                "restoreColumnName": "",
                "restoreColumnIndex": 0
            },
            "errorLimit": {
                "record": 100
            },
            "speed": {
                "bytes": 0,
                "channel": 1
            }
        }
    }
}
```
<a name="KxOTY"></a>
#### 3、读取多个路径下的文件
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "parameter": {
                        "path": "/data/ftp/flinkx/dir1,/data/ftp/flinkx/dir2",
                        "protocol": "sftp",
                        "port": 22,
                        "isFirstLineHeader": true,
                        "host": "localhost",
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
                        "password": "pass",
                        "fieldDelimiter": ",",
                        "encoding": "utf-8",
                        "username": "user"
                    },
                    "name": "ftpreader"
                },
                "writer": {
                    "parameter": {},
                    "name": "streamwriter"
                }
            }
        ],
        "setting": {
            "restore": {
                "maxRowNumForCheckpoint": 0,
                "isRestore": false,
                "restoreColumnName": "",
                "restoreColumnIndex": 0
            },
            "errorLimit": {
                "record": 100
            },
            "speed": {
                "bytes": 0,
                "channel": 1
            }
        }
    }
}
```


