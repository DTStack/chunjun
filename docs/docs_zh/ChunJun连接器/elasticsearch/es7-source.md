# ElasticSearch Source

## 一、介绍
ElasticSearch Source插件支持从现有的ElasticSearch集群读取指定index中的数据。

## 二、支持版本
Elasticsearch 7.x

## 三、插件名称

| 类型|名称|
| --- | --- |
| Sync | elasticsearch7reader |
| SQL | elasticsearch7-x |


## 四、参数说明
### 1、数据同步

- **hosts**
   - 描述：Elasticsearch集群的连接地址。 eg: "localhost:9200"，多个地址用分号作为分隔符。
   - 必选：是
   - 参数类型：List
   - 默认值：无
  


- **index**
   - 描述：指定访问Elasticsearch集群的index名称
   - 必选：是
   - 参数类型：String
   - 默认值：无



- **username**
   - 描述：开启basic认证之后的用户名
   - 必须：否
   - 参数类型：String
   - 默认值：无



- **password**
   - 描述：开启basic认证之后的密码
   - 必须：否
   - 参数类型：String
   - 默认值：无



- **batchSize**
   - 描述：批量读取数据的条数
   - 必须：否
   - 参数类型：Integer
   - 默认值：1



- **column**
   - 描述：需要读取的字段
   - 注意：不支持*格式
   - 参数类型：List  
   - 格式：
  
    ```
    "column": [{
      "name": "col", -- 字段名称
      "type": "text", -- 字段类型，当name没有指定时，则返回常量列，值为value指定
      "value": "value" -- 常量列的值
    }]
   ```
   - 默认值：无


 
- **connectTimeout**
    - 描述：ES Client最大连接超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：5000



- **socketTimeout**
    - 描述：ES Client最大socket超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：1800000



- **keepAliveTime**
    - 描述：ES Client会话最大保持时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：5000



- **requestTimeout**
    - 描述：ES Client最大请求超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：2000



- **maxConnPerRoute**
    - 描述：每一个路由值的最大连接数量
    - 必须：否
    - 参数类型：Integer
    - 默认值：10



- **sslConfig**
  - 描述：开启ssl连接认证需要的配置项    
      - useLocalFile：是否使用本地文件
      - fileName：文件名，使用本地文件时，文件路径为：filePath/fileName，使用sftp时，文件路径为：path/fileName
      - filePath：文件所在上级目录
      - keyStorePass：使用证书文件的密码，在生成证书文件时所指定的密码，若无则无需配置
      - type：证书类型，目前支持ca(ca.crt)和pkcs12(xxx.p12)两种类型的证书文件，可选值：ca/pkcs12
      - sftpConf：sftp配置 
   - 必须：否
   - 参数类型：Map  
   - 示例：
    ```
    "sslConfig": {
      "useLocalFile":false,
      "fileName":"ca.crt",
      "filePath":"/Users/edy/Downloads",
      "keyStorePass":"",
      "type":"ca",
      "sftpConf": {
        "path":"/data/sftp/ssl",
        "password":"dtstack",
        "port":"22",
        "auth":"1",
        "host":"127.0.0.1",
        "username":"root"
      }
    }
    ```
  

#### 2、SQL

- **hosts**
   - 描述：Elasticsearch集群的连接地址。eg: "localhost:9200"，多个地址用分号作为分隔符。
   - 必选：是
   - 参数类型：List
   - 默认值：无



- **index**
   - 描述：指定访问Elasticsearch集群的index名称
   - 必选：是
   - 参数类型：String
   - 默认值：无



- **username**
   - 描述：开启basic认证之后的用户名
   - 必须：否
   - 参数类型：String
   - 默认值：无



- **password**
   - 描述：开启basic认证之后的密码
   - 必须：否
   - 参数类型：String
   - 默认值：无



- **sink.bulk-flush.max-actions**
   - 描述：一次性读取es数据的条数
   - 必须：否
   - 参数类型：Integer
   - 默认值：1



- **client.connect-timeout**
    - 描述：ES Client最大连接超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：5000



- **client.socket-timeout**
    - 描述：ES Client最大socket超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：1800000



- **client.keep-alive-time**
    - 描述：ES Client会话最大保持时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：5000



- **client.request-timeout**
    - 描述：ES Client最大请求超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：2000
      


- **client.max-connection-per-route**
    - 描述：每一个路由值的最大连接数量
    - 必须：否
    - 参数类型：Integer
    - 默认值：10
  


- **security.ssl-keystore-file**      
    - 描述：ssl keystore认证文件名
    - 必须: 否
    - 参数类型：String
    - 默认值：无
      


- **security.ssl-keystore-password**  
    - 描述：ssl keystore认证文件密码，如果存在的话
    - 必须：否
    - 参数类型：String
    - 默认值：无
  


- **security.ssl-type**
    - 描述：证书类型，目前支持ca(ca.crt)和pkcs12(xxx.p12)两种类型的证书文件，可选值：ca/pkcs12
    - 必须：否
    - 参数类型：String
    - 默认值：无

## 五、数据类型

|是否支持 | 类型名称 |
| --- | --- |
| 支持 |INTEGER,FLOAT,DOUBLE,LONG,DATE,TEXT,BYTE,BINARY,OBJECT,NESTED|
| 不支持 | IP,GEO_POINT,GEO_SHAPE|

## 六、脚本示例
见项目内`chunjun-examples`文件夹。
