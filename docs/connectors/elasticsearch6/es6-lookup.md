# 一、介绍

ElasticSearch Lookup插件支持从现有的ElasticSearch集群读取指定index中的数据，并作为维表与主表进行关联。目前维表支持全量维表和异步维表。

# 二、支持版本

Elasticsearch 6.x

# 三、插件名称

|类型|名称|
| --- | --- |
| SQL | es-x |

​<br />

# 四、参数说明

## 1、SQL

- hosts
    - 描述：Elasticsearch集群的连接地址。eg: 'http://host_name:9092;http://host_name:9093'
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
- lookup.cache-type
    - 描述：维表类型。eg: all 或者 lru
    - 必须：否
    - 参数类型：String
    - 默认值：LRU
- lookup.cache-period
    - 描述：全量维表周期时间
    - 必须：否
    - 参数类型：Long
    - 默认值：3600 * 1000L
- lookup.cache.max-rows
    - 描述：维表缓存的最大条数
    - 必须：否
    - 参数类型：Long
    - 默认值：1000L
- lookup.cache.ttl
    - 描述：缓存生命周期
    - 必须：否
    - 参数类型：Long
    - 默认值：60 * 1000L
- lookup.error-limit
    - 描述：维表数据不合规条数
    - 必须：否
    - 参数类型：Long
    - 默认值：Long.MAX_VALUE
- lookup.fetch-size
    - 描述：抓取维表数据的条数
    - 必须：否
    - 参数类型：Integer
    - 默认值：1000L
- lookup.parallelism
    - 描述：维表并行度
    - 必须：否
    - 参数类型：Integer
    - 默认值：1

# 五、数据类型

|是否支持 | 类型名称 |
| --- | --- |
| 支持 |INTEGER,SMALLINT,DECIMAL,TIMESTAM DOUBLE,FLOAT,DATE,VARCHAR,VARCHAR,TIMESTAMP,TIME,BYTE|
| 不支持 | IP，binary, nested, object|

# 六、脚本示例

见项目内`flinkx-examples`文件夹。
