# ElasticSearch Lookup

## 一、介绍
ElasticSearch Lookup插件支持从现有的ElasticSearch集群读取指定index中的数据，并作为维表进行与主表进行关联。目前维表支持全量维表和异步维表。

## 二、支持版本

Elasticsearch 7.x

## 三、插件名称


|类型|名称|
| --- | --- |
| SQL | elasticsearch7-x |

## 四、参数说明
### 1、SQL

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
  

- **lookup.cache-type**
   - 描述：维表类型。eg: all 或者 lru
   - 必须：否
   - 参数类型：String
   - 默认值：LRU
  

- **lookup.cache-period**
   - 描述：全量维表周期时间
   - 必须：否
   - 参数类型：Long
   - 默认值：3600 * 1000L
  

- **lookup.cache.max-rows**
   - 描述：维表缓存的最大条数
   - 必须：否
   - 参数类型：Long
   - 默认值：1000L
  

- **lookup.cache.ttl**
   - 描述：缓存生命周期
   - 必须：否
   - 参数类型：Long
   - 默认值：60 * 1000L
  

- **lookup.error-limit**
   - 描述：维表数据不合规条数
   - 必须：否
   - 参数类型：Long
   - 默认值：Long.MAX_VALUE
  

- **lookup.fetch-size**
   - 描述：抓取维表数据的条数
   - 必须：否
   - 参数类型：Integer
   - 默认值：1000L
  

- **lookup.parallelism8**
   - 描述：维表并行度
   - 必须：否
   - 参数类型：Integer
   - 默认值：1


## 五、数据类型
|是否支持 | 类型名称 |
| --- | --- |
| 支持 |INTEGER,FLOAT,DOUBLE,LONG,DATE,TEXT,BYTE,BINARY,OBJECT,NESTED|
| 不支持 | IP,GEO_POINT,GEO_SHAPE|

## 六、脚本示例
见项目内`chunjun-examples`文件夹。
