# ElasticSearch Reader

<a name="c6v6n"></a>
## 一、插件名称
名称：**esreader**
<a name="jVb3v"></a>
## 二、支持的数据源版本
**Elasticsearch 6.X**
<a name="2lzA4"></a>
## 三、参数说明<br />

- **address**
  - 描述：Elasticsearch地址，单个节点地址采用host:port形式，多个节点的地址用逗号连接
  - 必选：是
  - 默认值：无



- **username**
  - 描述：Elasticsearch认证用户名
  - 必选：否
  - 默认值：无



- **password**
  - 描述：Elasticsearch认证密码
  - 必选：否
  - 默认值：无



- **query**
  - 描述：Elasticsearch查询表达式，[查询表达式](https://www.elastic.co/guide/cn/elasticsearch/guide/current/query-dsl-intro.html)
  - 必选：否
  - 默认值：无，默认为全查询



- **batchSize**
  - 描述：每次读取数据条数
  - 必选：否
  - 默认值：10



- **timeout**
  - 描述：连接超时时间
  - 必选：否
  - 默认值：无



- **index**
  - 描述：要查询的索引名称
  - 必选：否
  - 默认值：无



- **type**
  - 描述：要查询的类型
  - 必选：否
  - 默认值：无



- **column**
  - 描述：读取elasticsearch的查询结果的若干个列，每列形式如下
    - name：字段名称，可使用多级格式查找
    - type：字段类型，当name没有指定时，则返回常量列，值为value指定
    - value：常量列的值
  - 必选：是
  - 默认值：无



<a name="1LBc2"></a>
## 四、配置示例
```json
{
  "job" : {
    "content" : [ {
      "reader": {
        "name": "esreader",
        "parameter": {
          "address": "kudu4:9200",
          "query": {
            "match_all": {}
          },
          "index": "tudou",
          "type": "doc",
          "batchSize": 1000,
          "username": "elastic",
          "password": "abc123",
          "timeout": 10,
          "column": [
            {
              "name": "id",
              "type": "integer"
            },{
              "name": "user_id",
              "type": "integer"
            },{
              "name": "name",
              "type": "string"
            }
          ]
        }
      },
      "writer": {
        "name": "streamwriter",
        "parameter": {
          "print": true
        }
      }
    }
    ],
    "setting" : {
      "restore" : {
        "maxRowNumForCheckpoint" : 0,
        "isRestore" : false,
        "restoreColumnName" : "",
        "restoreColumnIndex" : 0
      },
      "errorLimit" : {
        "record" : 0,
        "percentage" : 0
      },
      "speed" : {
        "bytes" : 1048576,
        "channel" : 1
      }
    }
  }
}
```
