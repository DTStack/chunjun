# Ftp Source

## 一、介绍
ftp source

## 二、数据源配置
FTP服务搭建<br />windows：[地址](https://help.aliyun.com/document_detail/92046.html?spm=a2c4g.11186623.6.1185.6371dcd5DOfc5z)<br />linux：[地址](https://help.aliyun.com/document_detail/92048.html?spm=a2c4g.11186623.6.1184.7a9a2dbcRLDNlf)<br />sftp服务搭建<br />windows：[地址](http://www.freesshd.com/)<br />linux：[地址](https://yq.aliyun.com/articles/435356?spm=a2c4e.11163080.searchblog.102.576f2ec1BVgWY7)<br />

## 三、插件名称
| sync | ftpsource, ftpreader |
| --- | --- |
| sql | ftp-x |

## 四、参数说明

### 1、sync

- **path**
   - 描述：数据文件路径
   - 必选：是
   - 字段类型：String
   - 默认值：无
<br />

- **protocol**
   - 描述：服务器访问协议，目前支持ftp、sftp
   - 必选：是
   - 字段类型：String
   - 默认值：无
<br />

- **host**
   - 描述：ftp服务器地址
   - 必选：是
   - 字段类型：String
   - 默认值：无
<br />

- **port**
   - 描述：ftp服务器端口
   - 必选：否
   - 字段类型：int
   - 默认值：若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21
<br />

- **username**
   - 描述：ftp服务器登陆用户名
   - 必选：是
   - 字段类型：String
   - 默认值：无
<br />

- **password**
   - 描述：ftp服务器登陆密码
   - 必选：否
   - 字段类型：String
   - 默认值：无
<br />

- **privateKeyPath**
   - 描述：sftp私钥文件路径
   - 必选：否
   - 字段类型：String
   - 默认值：无
<br />

- **connectPattern**
   - 描述：protocol为ftp时的连接模式，可选PASV和PORT，参数含义可参考：[模式说明](https://blog.csdn.net/qq_16038125/article/details/72851142)
   - 必选：否
   - 字段类型：String
   - 默认值：PASV
<br />

- **fieldDelimiter**
   - 描述：读取的字段分隔符
   - 必选：否
   - 字段类型：String
   - 默认值：,
<br />

- **encoding**
   - 描述：读取文件的编码配置
   - 必选：否
   - 字段类型：String
   - 默认值：UTF-8
<br />

- **controlEncoding**
   - 描述：FTP客户端编码格式
   - 必选：否
   - 字段类型：String
   - 默认值：UTF-8
<br />

- **isFirstLineHeader**
   - 描述：首行是否为标题行，如果是则不读取第一行
   - 必选：否
   - 字段类型：boolean
   - 默认值：false
<br />

- **timeout**
   - 描述：连接超时时间，单位毫秒
   - 必选：否
   - 字段类型：String
   - 默认值：5000
<br />

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
<br />

- **fileType**
    - 描述：读取的文件类型，默认取文件后缀名，支持CSV,TXT,EXCEL
    - 必选：否
    - 字段类型：string
    - 默认值：无
<br />

- **compressType**
    - 描述：文件压缩类型,支持ZIP
    - 必选：否
    - 字段类型：string
    - 默认值：无
<br />

- **fileConfig**
    - 描述：文件参数配置
    - 必选：否
    - 字段类型：Map
    - 默认值：无
    - 示例：
        - csv文件是否进行trim：`"fileConfig":{"trimWhitespace":true}`
  <br />

#### 2、sql

- **connector**
   - 描述：ftp-x
   - 必选：是
   - 字段类型：String
   - 默认值：无
<br />

- **path**
   - 描述：文件路径
   - 必选：是
   - 字段类型：String
   - 默认值：无
<br />

- **protocol**
   - 描述：服务器访问协议，目前支持ftp、sftp
   - 必选：是
   - 字段类型：String
   - 默认值：无
<br />

- **host**
   - 描述：服务地地址
   - 必选：是
   - 字段类型：String
   - 默认值：无
<br />

- **port**
   - 描述：ftp服务器端口
   - 必选：否
   - 字段类型：int
   - 默认值：若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21
<br />

- **username**
   - 描述：服务器登陆用户名
   - 必选：是
   - 字段类型：String
   - 默认值：无
<br />

- **password**
   - 描述：服务器登陆密码
   - 必选：否
   - 字段类型：String
   - 默认值：无
<br />

- **format**
   - 描述：文件的类型，和原生flink保持一致，支持原生所有类型
   - 必选：否
   - 参数类型：string
   - 默认值：csv
<br />

- **connect-pattern**
   - 描述：protocol为ftp时的连接模式，可选PASV和PORT
   - 必选：否
   - 字段类型：String
   - 默认值：PASV
<br />

- **timeout**
   - 描述：连接超时时间，单位毫秒
   - 必选：否
   - 字段类型：String
   - 默认值：5000


## 五、数据类型
| 支持 | BOOLEAN |
| --- | --- |
|  | TINYINT |
|  | SMALLINT |
|  | INT |
|  | BIGINT |
|  | FLOAT |
|  | DOUBLE |
|  | DECIMAL |
|  | STRING |
|  | VARCHAR |
|  | CHAR |
|  | TIMESTAMP |
|  | DATE |


## 六、配置示例
见项目内`chunjun-examples`文件夹
