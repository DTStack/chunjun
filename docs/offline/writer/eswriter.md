# ElasticSearch Writer

<a name="c6v6n"></a>
## 一、插件名称
名称：**eswriter**<br />
<a name="jVb3v"></a>
## 二、支持的数据源版本
**Elasticsearch 6.X**<br />
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



- **index**
  - 描述：Elasticsearch 索引值
  - 必选：是
  - 默认值：无



- **type**
  - 描述：Elasticsearch 索引类型
  - 必选：是
  - 默认值：无



- **column**
  - 描述：写入elasticsearch的若干个列，每列形式如下
```
  {
      "name": "列名",
      "type": "列类型"
  }
```

  - 必选：是
  - 默认值：无



- **idColumns**
  - 描述：用于构造文档id的若干个列，每列形式如下

普通列
```
{
  "index": 0,  // 前面column属性中列的序号，从0开始
  "type": "string" 列的类型，默认为string
}
```
常数列
```
{
  "value": "ffff", // 常数值
  "type": "string" // 常数列的类型，默认为string
}
```

  - 必选：否
  - 注意：
    - 如果不指定idColumns属性，则会随机产生文档id
    - 如果指定的字段值存在重复或者指定了常数，按照es的逻辑，同样值的doc只会保留一份
  - 默认值：无



- **bulkAction**
  - 描述：批量写入的记录条数
  - 必选：是
  - 默认值：100



- **timeout**
  - 描述：连接超时时间，如果bulkAction指定的数值过大，写入数据可能会超时，这时可以配置超时时间
  - 必选：否
  - 默认值：无



<a name="1LBc2"></a>
## 四、配置示例
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "name": "id",
                "type": "id"
              },
              {
                "name": "user_id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              }
            ],
            "sliceRecordCount": ["100"]
          }
        },
        "writer": {
          "name": "eswriter",
          "parameter": {
            "address": "172.16.8.193:9200",
            "username": "elastic",
            "password": "abc123",
            "index": "tudou",
            "type": "doc",
            "bulkAction": 100,
            "timeout": 100,
            "idColumn": [
              {
                "index": 0,
                "type": "integer"
              }
            ],
            "column": [
              {
                "name": "id",
                "type": "integer"
              },
              {
                "name": "user_id",
                "type": "integer"
              },
              {
                "name": "name",
                "type": "string"
              }
            ]
          }
        }
      }
    ],
    "setting": {
      "restore": {
        "maxRowNumForCheckpoint": 0,
        "isRestore": false,
        "restoreColumnName": "",
        "restoreColumnIndex": 0
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0
      },
      "speed": {
        "bytes": 1048576,
        "channel": 1
      }
    }
  }
}
```
