# FTP读取插件（ftpreader）

## 1. 配置样例

```
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 1,
                 "bytes": 10000
            },
            "errorLimit": {
                "record": 0,
                "percentage": 50
            }
        },
        "content": [
            {
                "reader": {
                    "name": "ftpreader",
                    "parameter": {
                        "protocol": "sftp",
                        "host": "node01" ,
                        "port": 22,
                        "username": "mysftp",
                        "password": "oh1986mygod",
                        "column": [
                            {
                                "index": 0
                            },
                            {
                                "index": 1
                            },
                            {
                                "value": "youcan",
                                "type": "string"
                            }
                        ],
                        "path": "/upload",
                        "encoding": "UTF-8",
                        "fieldDelimiter": "\\t",
                        "isFirstLineHeader":true
                    }
                },
                "writer": {
                    "parameter": {
                        "password": "abc123",
                        "column": [
                            "col1",
                            "col2",
                            "col3"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://172.16.8.104:3306/test?charset=utf8",
                                "table": [
                                    "sb5"
                                ]
                            }
                        ],
                        "writeMode": "insert",
                        "username": "dtstack"
                    },
                    "name": "mysqlwriter"
                }
            }
        ]
    }
}

```

## 2. 参数说明

* **protocol**

	* 描述：ftp服务器协议，目前支持传输协议有ftp和sftp。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **host**

	* 描述：ftp服务器地址。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **port**

	* 描述：ftp服务器端口。 <br />

	* 必选：否 <br />

	* 默认值：若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21 <br />

* **connectPattern**

	* 描述：连接模式（主动模式或者被动模式）。该参数只在传输协议是标准ftp协议时使用，值只能为：PORT (主动)，PASV（被动）。两种模式主要的不同是数据连接建立的不同。对于Port模式，是客户端在本地打开一个端口等服务器去连接建立数据连接，而Pasv模式就是服务器打开一个端口等待客户端去建立一个数据连接。<br />

	* 必选：否 <br />

	* 默认值：PASV<br />

* **username**

	* 描述：ftp服务器访问用户名。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：ftp服务器访问密码。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **path**

	* 描述：远程FTP文件系统的路径信息，注意这里可以支持填写多个路径。 <br />

	* 必选：是 <br />

	* 默认值：/ <br />

* **column**

	* 描述：读取字段列表，type指定源数据的类型，index指定当前列来自于文本第几列(以0开始)，value指定当前类型为常量。
	

		用户可以指定column字段信息，配置如下：

		```json
		{
           "index": 0    //从远程FTP文件文本第一列获取int字段
        },
        {
           "type": "string",
           "value": "alibaba"  //从FtpReader内部生成alibaba的字符串字段作为当前字段
        }
		```

		对于用户指定Column信息，type必须填写，index/value必须选择其一。

	* 必选：是 <br />

	* 默认值：全部按照string类型读取 <br />

* **fieldDelimiter**

	* 描述：读取的字段分隔符 <br />

	* 必选：是 <br />

	* 默认值：, <br />

* **encoding**

	* 描述：读取文件的编码配置。<br />

 	* 必选：否 <br />

 	* 默认值：utf-8 <br />
 	
* **isFirstLineHeader**

	* 描述：首行是否为标题行，如果是则不读取第一行。<br />

 	* 必选：否 <br />

 	* 默认值：false <br />


