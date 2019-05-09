# FTP读取插件（ftpreader）

## 1. 配置样例

```
{
    "job": {
        "setting": {},
        "content": [{
            "reader": {
                "name": "ftpreader",
                "parameter": {
                    "protocol": "sftp",
                    "host": "127.0.0.1",
                    "port": 22,
                    "username": "username",
                    "password": "password",
                    "column": [{
                        "index": 0,
                        "type": "",
                        "value": "value"
                    }],
                    "path": "/upload",
                    "encoding": "UTF-8",
                    "fieldDelimiter": ",",
                    "isFirstLineHeader": true
                }
            },
            "writer": {}
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

* **connectPattern**
  
  * 描述：连接模式（主动模式或者被动模式）。该参数只在传输协议是标准ftp协议时使用，值只能为：PORT (主动)，PASV（被动）。两种模式主要的不同是数据连接建立的不同。对于Port模式，是客户端在本地打开一个端口等服务器去连接建立数据连接，而Pasv模式就是服务器打开一个端口等待客户端去建立一个数据连接。
  
  * 必选：否 
  
  * 默认值：PASV

* **username**
  
  * 描述：ftp服务器访问用户名。
  
  * 必选：是
  
  * 默认值：无

* **password**
  
  * 描述：ftp服务器访问密码。
  
  * 必选：是
  
  * 默认值：无 

* **path**
  
  * 描述：远程FTP文件系统的路径信息，注意这里可以支持填写多个路径。
  
  * 必选：是
  
  * 默认值：/

* **column**
  
  * 描述：需要读取的字段。
  
  * 格式：支持2中格式
    
    1.读取全部字段，如果字段数量很多，可以使用下面的写法：
    
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
  
  * 属性说明:
    
    * index：字段索引
    
    * type：字段类型，ftp读取的为文本文件，本质上都是字符串类型，这里可以指定要转成的类型
    
    * format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
    
    * value：如果没有指定index，则会把value的值作为常量列返回，如果指定了index，当读取的字段的值为null时，会以此value值作为默认值返回
  
  * 必选：是
  
  * 默认值：无

* **fieldDelimiter**
  
  * 描述：读取的字段分隔符 
  
  * 必选：是 
  
  * 默认值：, 

* **encoding**
  
  * 描述：读取文件的编码配置。
  * 必选：否
  * 默认值：utf-8

* **isFirstLineHeader**
  
  * 描述：首行是否为标题行，如果是则不读取第一行。
  * 必选：否
  * 默认值：false
