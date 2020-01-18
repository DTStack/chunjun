# Cassandra读取插件（cassandrareader）

## 1. 配置样例

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "parameter": {
                        "host": "127.0.0.1",
                        "port":9042,
                        "username":"username",
                        "password":"password",
                        "useSSL":false,

                        "keyspace": "tp",
                        "table": "emp",
                        "column": [
                            "emp_id",
                            "emp_city",
                            "emp_name",
                            "emp_phone",
                            "emp_sal"
                        ],
                        "allowFiltering":false,
                        "where":null,
                        "connectionsPerHost":8,
                        "maxPendingPerConnection":128,
                        "consistancyLevel":null

                    },
                    "name": "cassandrareader"
                },
                "writer": {
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": 1,
                "bytes": 0
            },
            "errorLimit": {
                "record": 10000,
                "percentage": 100
            }
        }
    }
}
```

## 2. 参数说明

- **host**
  
  - 描述：数据库地址
  
  - 必选：是
  
  - 默认值：无

- **port**
  
  - 描述：端口
  
  - 必选：否
  
  - 默认值：9042

- **username**
  
  - 描述：用户名
  
  - 必选：否
  
  - 默认值：无

- **password**
  
  - 描述：密码
  
  - 必选：否
  
  - 默认值：无

- **column**
  
  - 描述：要读取的字段
  
  - 必选：否
  
  - 默认值：无

- **keyspace**
  
  - 描述：需要同步的表所在的keyspace
  
  - 必选：是
  
  - 默认值：无

- **table**
  
  - 描述：要查询的表
  
  - 必选：是
  
  - 默认值：无

- **allowFiltering**
  
  - 描述：是否在服务端过滤数据。
  
  - 必选：否
  
  - 默认值：false

- **where**
  
  - 描述：过滤条件where之后的表达式
  
  - 必选：否
  
  - 默认值：无

- **consistancyLevel**
  
  - 描述：数据一致性级别。可选ONE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|ALL|ANY|TWO|THREE|LOCAL_ONE
  
  - 必选：否
  
  - 默认值：无
