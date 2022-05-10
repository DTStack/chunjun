# 一、介绍

ElasticSearch Sink插件支持将数据写入到指定的index中。 ​

# 二、支持版本

Elasticsearch 7.x ​

# 三、插件名称

| 类型|名称|
| ---- | ----|
| Sync | elasticsearch7writer |
| SQL | elasticsearch7-x |

​

# 四、参数说明

## 1、数据同步

- hosts
    - 描述：Elasticsearch集群的连接地址。eg: ["localhost:9200"]
    - 必选：是
    - 参数类型：List<String>
    - 默认值：无
- index
    - 描述：指定访问Elasticsearch集群的index名称
    - 必选：是
    - 参数类型：String
    - 默认值：无
- username
    - 描述：开启basic认证之后的用户名
    - 必须：否
    - 参数类型：String
    - 默认值：无
- password
    - 描述：开启basic认证之后的密码
    - 必须：否
    - 参数类型：String
    - 默认值：无
- batchSize
    - 描述：批量写入数据的条数
    - 必须：否
    - 参数类型：Integer
    - 默认值：1
- keyDelimiter
    - 描述：文档id之间的分隔符号，eg:“${col1}_${col2}”
    - 必须：否
    - 参数类型：无
    - 默认值："_"
- column
    - 描述：需要读取的字段
    - 注意：不支持*格式
    - 格式：
- connectTimeout
    - 描述：ES Client最大连接超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：5000
- socketTimeout
    - 描述：ES Client最大socket超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：1800000
- keepAliveTime
    - 描述：ES Client会话最大保持时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：5000
- requestTimeout
    - 描述：ES Client最大请求超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：2000
- maxConnPerRoute
    - 描述：每一个路由值的最大连接数量
    - 必须：否
    - 参数类型：Integer
    - 默认值：10

```
"column": [{
    "name": "col", -- 字段名称，可使用多级格式查找
    "type": "string", -- 字段类型，当name没有指定时，则返回常量列，值为value指定
    "value": "value" -- 常量列的值
}]
```

​

## 2、SQL

- hosts
    - 描述：Elasticsearch集群的连接地址。eg: "localhost:9200", 多地址用分号分隔符隔开。
    - 必选：是
    - 参数类型：List<String>
    - 默认值：无
- index
    - 描述：指定访问Elasticsearch集群的index名称
    - 必选：是
    - 参数类型：String
    - 默认值：无
- username
    - 描述：开启basic认证之后的用户名
    - 必须：否
    - 参数类型：String
    - 默认值：无
- password
    - 描述：开启basic认证之后的密码
    - 必须：否
    - 参数类型：String
    - 默认值：无
- sink.bulk-flush.max-actions
    - 描述：批量写入es数据的条数
    - 必须：否
    - 参数类型：Integer
    - 默认值：1
- document-id.key-delimiter
    - 描述：文档id之间的分隔符号，eg:“${col1}_${col2}”
    - 必须：否
    - 参数类型：String
    - 默认值："_"
- client.connect-timeout
    - 描述：ES Client最大连接超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：5000
- client.socket-timeout
    - 描述：ES Client最大socket超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：1800000
- client.keep-alive-time
    - 描述：ES Client会话最大保持时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：5000
- client.request-timeout
    - 描述：ES Client最大请求超时时间。
    - 必须：否
    - 参数类型：Integer
    - 默认值：2000
- client.max-connection-per-route
    - 描述：每一个路由值的最大连接数量
    - 必须：否
    - 参数类型：Integer
    - 默认值：10

# 五、数据类型

|是否支持 | 类型名称 |
| --- | --- |
| 支持 |INTEGER,SMALLINT,DECIMAL,TIMESTAM DOUBLE,FLOAT,DATE,VARCHAR,VARCHAR,TIMESTAMP,TIME,BYTE|
| 不支持 | IP，binary, nested, object|

# 六、脚本示例

见项目内`FlinkX：Local：Test`模块中的`demo文件夹。
