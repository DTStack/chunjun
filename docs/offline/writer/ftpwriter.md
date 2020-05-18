# FTP Writer

<a name="0eTYo"></a>
## 一、插件名称
名称：**ftpwriter**<br />

<a name="ijmKB"></a>
## 二、数据源版本
| 协议 | 是否支持 |
| --- | --- |
| FTP | 支持 |
| SFTP | 支持 |



<a name="WuquB"></a>
## 三、数据源配置
FTP服务搭建<br />windows：[地址](https://help.aliyun.com/document_detail/92046.html?spm=a2c4g.11186623.6.1185.6371dcd5DOfc5z)<br />linux：[地址](https://help.aliyun.com/document_detail/92048.html?spm=a2c4g.11186623.6.1184.7a9a2dbcRLDNlf)<br />sftp服务搭建<br />windows：[地址](http://www.freesshd.com/)<br />linux：[地址](https://yq.aliyun.com/articles/435356?spm=a2c4e.11163080.searchblog.102.576f2ec1BVgWY7)<br />

<a name="CAZZC"></a>
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



- **privateKeyPath**
  - 描述：私钥文件路径
  - 必选：否
  - 默认值：无

<br />

- **writeMode**
  - 描述：ftpwriter写入前数据清理处理模式：
    - append：追加
    - overwrite：覆盖
  - 注意：overwrite模式时会删除dtp当前目录下的所有文件
  - 必选：否
  - 默认值：append



- **isFirstLineHeader**
  - 描述：首行是否为标题行，如果是则不读取第一行
  - 必选：否
  - 默认值：false



- **timeout**
  - 描述：连接超时时间，单位毫秒
  - 必选：否
  - 默认值：5000



- **maxFileSize**
  - 描述：写入hdfs单个文件最大大小，单位字节
  - 必须：否
  - 默认值：1073741824‬（1G）



- **column**
  - 描述：需要读取的字段
  - 格式：指定具体信息：
```json
"column": [{
    "name": "col1",
    "type": "datetime"
}]
```

  - 属性说明:
    - name：字段名称
    - type：字段类型，ftp读取的为文本文件，本质上都是字符串类型，这里可以指定要转成的类型
  - 必选：是
  - 默认值：无



<a name="lplB9"></a>
## 五、使用示例
<a name="vZyir"></a>
#### 1、append模式写入
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "parameter": {
                        "column": [
                            {
                                "name": "col1",
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "type": "int"
                            }
                        ],
                        "sliceRecordCount": [
                            "100"
                        ]
                    },
                    "name": "streamreader"
                },
                "writer": {
                    "parameter": {
                        "path": "/data/ftp/flinkx",
                        "protocol": "sftp",
                        "port": 22,
                        "writeMode": "append",
                        "host": "localhost",
                        "column": [
                            {
                                "name": "col1",
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "type": "int"
                            }
                        ],
                        "password": "pass",
                        "fieldDelimiter": ",",
                        "encoding": "utf-8",
                        "username": "user"
                    },
                    "name": "ftpwriter"
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
<a name="JjqE3"></a>
#### 2、指定文件大小
```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "parameter": {
                        "column": [
                            {
                                "name": "col1",
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "type": "int"
                            }
                        ],
                        "sliceRecordCount": [
                            "0"
                        ]
                    },
                    "name": "streamreader"
                },
                "writer": {
                    "parameter": {
                        "path": "/data/ftp/flinkx",
                        "protocol": "sftp",
                        "port": 22,
                        "writeMode": "append",
                        "host": "localhost",
                        "column": [
                            {
                                "name": "col1",
                                "type": "string"
                            },
                            {
                                "name": "col2",
                                "type": "string"
                            },
                            {
                                "name": "col3",
                                "type": "int"
                            },
                            {
                                "name": "col4",
                                "type": "int"
                            }
                        ],
                        "password": "pass",
                        "fieldDelimiter": ",",
                        "encoding": "utf-8",
                        "username": "user",
                        "maxFileSize" : 5242880
                    },
                    "name": "ftpwriter"
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


