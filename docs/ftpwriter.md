# FTP写入插件（ftpwriter）

## 1. 配置样例

```
{
    "job": {
        "setting": {},
        "content": [{
            "reader": {},
            "writer": {
                "name": "ftpwriter",
                "parameter": {
                    "protocol": "sftp",
                    "host": "127.0.0.1",
                    "port": 22,
                    "username": "username",
                    "password": "password",
                    "writeMode": "overwrite",
                    "path": "/sftp",
                    "fieldDelimiter": ",",
                    "connectPattern": "PASV",
                    "column": [{
                        "type": "string"
                    }]
                }
            }
        }]
    }
}
```

## 2. 参数说明

* **protocol**
  
  * 描述：ftp服务器协议，目前支持传输协议有ftp和sftp。
  
  * 必选：是 
  
  * 默认值：无 

* **host**
  
  * 描述：ftp服务器地址。
  
  * 必选：是 
  
  * 默认值：无

* **port**
  
  * 描述：ftp服务器端口。 
  
  * 必选：否 
  
  * 默认值：若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21

* **username**
  
  * 描述：ftp服务器访问用户名。
  
  * 必选：是 
  
  * 默认值：无 

* **password**
  
  * 描述：ftp服务器访问密码。
  
  * 必选：是 
  
  * 默认值：无 

* **connectPattern**
  
  * 描述：连接模式（主动模式或者被动模式）。该参数只在传输协议是标准ftp协议时使用，值只能为：PORT (主动)，PASV（被动）。两种模式主要的不同是数据连接建立的不同。对于Port模式，是客户端在本地打开一个端口等服务器去连接建立数据连接，而Pasv模式就是服务器打开一个端口等待客户端去建立一个数据连接。
  
  * 必选：否 
  
  * 默认值：PASV

* **path**
  
  * 描述：FTP文件系统的路径信息，FtpWriter会写入Path目录下属多个文件。
  
  * 必选：是
  
  * 默认值：无 

* **writeMode**
  
  * 描述：FtpWriter写入前数据清理处理模式： 
    * overwrite，覆盖
    * append，追加
  * 必选：是
  * 默认值：无

* **fieldDelimiter**
  
  * 描述：写入的字段分隔符
  
  * 必选：否 
  
  * 默认值：, 

* **encoding**
  
  * 描述：读取文件的编码配置。
  * 必选：否
  * 默认值：utf-8
