# 一、介绍
ElasticSearch Sink插件支持将数据写入到指定的index中去。
​

# 二、支持版本
Elasticsearch 5.x
​

# 三、插件名称


| 类型|名称|
| ---- | ----|
| Sync | elasticsearch5writer |
| SQL | elasticsearch5-x |

​

# 四、参数说明


## 1、数据同步

- hosts
   - 描述：Elasticsearch集群的连接地址。eg: ["localhost:9300"]
   - 必选：是
   - 参数类型：List<String>
   - 默认值：无
- index
   - 描述：指定访问Elasticsearch集群的index名称
   - 必选：是
   - 参数类型：String
   - 默认值：无
- type
   - 描述：指定访问Elasticsearch集群的index下的type名称
   - 必选：是
   - 参数类型：String
   - 默认值：无
- cluster
   - 描述：指定访问Elasticsearch的集群名称
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
   - 描述：Elasticsearch集群的连接地址。eg: ["localhost:9300"]
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
- document-type
   - 描述：指定访问Elasticsearch集群的index下的type名称
   - 必选：是
   - 参数类型：String
   - 默认值：无
- cluster
   - 描述：指定访问Elasticsearch的集群名称
   - 必选：是
   - 参数类型：String
   - 默认值：无
- password
   - 描述：开启basic认证之后的密码
   - 必须：否
   - 参数类型：String
   - 默认值：无
- bulk-flush.max-actions
   - 描述：一次性读取es数据的条数
   - 必须：否
   - 参数类型：Integer
   - 默认值：1
- document-id.key-delimiter
   - 描述：文档id之间的分隔符号，eg:“${col1}_${col2}”
   - 必须：否
   - 参数类型：String
   - 默认值："_"

# 五、数据类型
| ​支持 | BOOLEAN |
| --- | --- |
|  | INTEGER |
|  | DECIMAL |
|  | TIMESTAMP |
|  | DOUBLE |
|  | FLOAT |
|  | DATE |
|  | VARCHAR |

# 六、脚本示例
见项目内`FlinkX：Local：Test`模块中的`demo文件夹。

