# Cassandra写入插件（cassandrareader）

## 1. 配置样例

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    
                },
                "writer": {
                    "name": "cassandrawriter",
                    "parameter": {
                        "host": "101.37.175.174",
                        "keyspace": "tp",
                        "table": "emp",
						"column": ["emp_id", "emp_name", "emp_city", "emp_phone", "emp_sal"]
                    }
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

- **asyncWrite**
  
  - 描述：是否同步写入数据。
  
  - 必选：否
  
  - 默认值：false

- **batchSize**
  
  - 描述：一次批量提交的记录数大小
  
  - 必选：否
  
  - 默认值：1

- **consistancyLevel**
  
  - 描述：数据一致性级别。可选ONE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|ALL|ANY|TWO|THREE|LOCAL_ONE
  
  - 必选：否
  
  - 默认值：无
