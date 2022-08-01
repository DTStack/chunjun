# Ftp Sink

## 一、介绍
ftp sink

## 二、数据源配置
FTP服务搭建:


windows：[地址](https://help.aliyun.com/document_detail/92046.html?spm=a2c4g.11186623.6.1185.6371dcd5DOfc5z)


linux：[地址](https://help.aliyun.com/document_detail/92048.html?spm=a2c4g.11186623.6.1184.7a9a2dbcRLDNlf)


sftp服务搭建:


windows：[地址](http://www.freesshd.com/)


linux：[地址](https://yq.aliyun.com/articles/435356?spm=a2c4e.11163080.searchblog.102.576f2ec1BVgWY7)
## 三、插件名称
| sync | ftpsink, ftpwriter |
| --- | --- |
| sql | ftp-x |


## 四、参数说明

### 1、sync

- **path**
   - 描述：数据文件路径
   - 必选：是
   - 字段类型：String
   - 默认值：无


- **protocol**
   - 描述：服务器访问协议，目前支持ftp、sftp
   - 必选：是
   - 字段类型：String
   - 默认值：无


- **host**
   - 描述：ftp服务器地址
   - 必选：是
   - 字段类型：String
   - 默认值：无


- **port**
   - 描述：ftp服务器端口
   - 必选：否
   - 字段类型：int
   - 默认值：若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21


- **username**
   - 描述：ftp服务器登陆用户名
   - 必选：是
   - 字段类型：String
   - 默认值：无


- **password**
   - 描述：ftp服务器登陆密码
   - 必选：否
   - 字段类型：String
   - 默认值：无


- **privateKeyPath**
   - 描述：sftp私钥文件路径
   - 必选：否
   - 字段类型：String
   - 默认值：无


- **connectPattern**
   - 描述：protocol为ftp时的连接模式，可选PASV和PORT，参数含义可参考：[模式说明](https://blog.csdn.net/qq_16038125/article/details/72851142)
   - 必选：否
   - 字段类型：String
   - 默认值：PASV


- **fieldDelimiter**
   - 描述：读取的字段分隔符
   - 必选：否
   - 字段类型：String
   - 默认值：','
    

- **ftpFileName**
   - 描述：远程FTP文件系统的文件名，只能写一个文件名。如果配置多并发度(channel > 1)，或者文件的大小超过 maxFileSize 配置的值，那么文件会被命名成 filename_0_0, filename_0_1,...,filename_1_0,filename_1_1 。第一个数字是 channel 的编号，第二个数字是拆分后的文件编号。
   - 必选：否
   - 字段类型：String
   - 默认值：无


- **timeout**
   - 描述：连接超时时间，单位毫秒
   - 必选：否
   - 字段类型：int
   - 默认值：5000


- **maxFileSize**
    - 描述：写入ftp单个文件最大大小，单位字节
    - 必须：否
    - 字段类型：long
    - 默认值：1073741824(1G)

    
- **writeMode**
    - 描述：ftpwriter写入前数据清理处理模式
        - append：追加
        - overwrite：覆盖 
    - 注意：追加写入是指在目录下追加写入一个文件a，并不是往某个文件追加写入内容；覆盖写入是指覆盖目录下所有文件，overwrite模式时会删除ftp当前目录下的所有文件
    - 必选：否
    - 字段类型：string
    - 默认值：append


- **column**
   - 描述：需要读取的字段
   - 注意：不支持*格式
   - 格式：
    ```json
        "column": [{
        "name": "col",
        "type": "string",
        "index":1,
        "isPart":false,
        "format": "yyyy-MM-dd hh:mm:ss",
        "value": "value"
        }]
    ```
   - 属性说明:
      - name：必选，字段名称
      - type：必选，字段类型，需要和数据文件中实际的字段类型匹配
      - index：非必选，字段在所有字段中的位置索引，从0开始计算，默认为-1，按照数组顺序依次读取，配置后读取指定字段列
      - isPart：非必选，是否是分区字段，如果是分区字段，会自动从path上截取分区赋值，默认为fale
      - format：非必选，按照指定格式，格式化日期
      - value：非必选，常量字段，将value的值作为常量列返回
   - 必选：是
   - 参数类型：数组
   - 默认值：无


### 2、sql

- **connector**
   - 描述：ftp-x
   - 必选：是
   - 字段类型：String
   - 默认值：无


- **path**
   - 描述：文件路径
   - 必选：是
   - 字段类型：String
   - 默认值：无


- **file-name**
   - 描述：文件名，如果不指定则随机生成
   - 必选：否
   - 字段类型：String
   - 默认值：无


- **protocol**
   - 描述：服务器访问协议，目前支持ftp、sftp
   - 必选：是
   - 字段类型：String
   - 默认值：无


- **host**
   - 描述：服务地地址
   - 必选：是
   - 字段类型：String
   - 默认值：无


- **port**
   - 描述：ftp服务器端口
   - 必选：否
   - 字段类型：int
   - 默认值：若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21


- **username**
   - 描述：服务器登陆用户名
   - 必选：是
   - 字段类型：String
   - 默认值：无


- **password**
   - 描述：服务器登陆密码
   - 必选：否
   - 字段类型：String
   - 默认值：无


- **format**
   - 描述：文件的类型，和原生flink保持一致，支持原生所有类型
   - 必选：否
   - 参数类型：string
   - 默认值：csv


- **connect-pattern**
   - 描述：protocol为ftp时的连接模式，可选PASV和PORT
   - 必选：否
   - 字段类型：String
   - 默认值：PASV


- **timeout**
   - 描述：连接超时时间，单位毫秒
   - 必选：否
   - 字段类型：String
   - 默认值：5000


- **write-mode**
    - 描述：ftpwriter写入前数据清理处理模式
        - append：追加
        - overwrite：覆盖
    - 注意：追加写入是指在目录下追加写入一个文件a，并不是往某个文件追加写入内容；覆盖写入是指覆盖目录下所有文件，overwrite模式时会删除ftp当前目录下的所有文件
    - 必选：否
    - 字段类型：string
    - 默认值：append
    

- **max-file-size**
    - 描述：写入ftp单个文件最大大小，单位字节
    - 必须：否
    - 字段类型：long
    - 默认值：1073741824(1G)


## 五、数据类型
|支持的类型|BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, STRING, VARCHAR, CHAR, BINARY, TIMESTAMP, DATETIME, TIME, DATE|
| --- | --- |
|不支持的类型|ARRAY, MAP, STRUCT|


## 六、脚本示例
见项目内`chunjun-examples`文件夹
