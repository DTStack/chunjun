# Redis写入插件（rediswriter）

## 1. 配置样例

```json
{
    "job":{
        "content":[{
            "reader":{},
            "writer":{
                "parameter":{
                    "hostPort":"localhost:6379",
                    "password": "密码",
                    "database":1,
                    "keyIndexes": [0,2],
                    "writeMode":"",
                    "keyFieldDelimiter": "\u0001",
                    "expireTime": 1000,
                    "timeout": 10000,
                    "dateFormat": "yyyy-MM-dd HH:mm:ss",
                    "type": "string",
                    "mode": "set",
                    "valueFieldDelimiter": "\u0001"
                },
                "name":"rediswriter"
            }
        }]
    }
}
```

## 2. 参数说明

* **name**
  
  * 描述：插件名，此处只能填rediswriter，否则Flinkx将无法正常加载该插件包。
  * 必选：是
  * 默认值：无

* **hostPort**
  
  * 描述：Redis的IP地址和端口
  
  * 必选：是
  
  * 默认值：localhost:6379

* **password**
  
  * 描述：数据源指定用户名的密码
  
  * 必选：是
  
  * 默认值：无

* **database**
  
  * 描述：要写入Redis数据库
  
  * 必选：否
  
  * 默认值：0

* **keyIndexes**
  
  * 描述：keyIndexes 表示源端哪几列需要作为 key（第一列是从 0 开始）。如果是第一列和第二列需要组合作为 key，那么 keyIndexes 的值则为 [0,1]。
  
  * 注意：配置 keyIndexes 后，Redis Writer 会将其余的列作为 value，如果您只想同步源表的某几列作为 key，某几列作为 value，不需要同步所有字段，那么在 Reader 插件端就指定好 column 作好列筛选即可。
  
  * 必选：是
  
  * 默认值：无

* **keyFieldDelimiter**
  
  * 描述：写入 Redis 的 key 分隔符。比如: key=key1\u0001id，如果 key 有多个需要拼接时，该值为必填项，如果 key 只有一个则可以忽略该配置项。
  
  * 必选：否
  
  * 默认值：\u0001

* **expireTime**
  
  * 描述：Redis value 值缓存失效时间（如果需要永久有效则可以不填该配置项）。
  
  * 注意：如果过期时间的秒数大于 60*60*24*30（即 30 天），则服务端认为是 Unix 时间，该时间指定了到未来某个时刻数据失效。否则为相对当前时间的秒数，该时间指定了从现在开始多长时间后数据失效。
  
  * 必选：否
  
  * 默认值：0（0 表示永久有效）

* **timeout**
  
  * 描述：写入 Redis 的超时时间。
  
  * 单位：毫秒
  
  * 必选：否
  
  * 默认值：30000

* **dateFormat**
  
  * 描述：写入 Redis 时，Date 的时间格式：”yyyy-MM-dd HH:mm:ss”
  
  * 必选：否
  
  * 默认值：将日期以long类型写入

* **writeMode**
  
  * 描述：写入模式，由于 Redis 的数据结构为key-value模式，因此只要key相同，就会覆盖value值
  
  * 必选：是
  
  * 所有选项：insert
  
  * 默认值：insert

* **valueFieldDelimiter**
  
  * 描述：该配置项是考虑了当源数据每行超过两列的情况（如果您的源数据只有两列即 key 和 value 时，那么可以忽略该配置项，不用填写），value 类型是 string 时，value 之间的分隔符，比如 value1\u0001value2\u0001value3。
  
  * 必选：否
  
  * 默认值：\u0001

* **type和mode**
  
  * 描述：type 表示 value 的类型，mode 表示在选定的数据类型下的写入模式。
  
  * 选项：string/list/set/zset/hash
    
    | type   | 描述      | mode  | 说明                           | 注意                                                                                                                                                                 |
    | ------ | ------- | ----- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
    | string | 字符串     | set   | 存储这个数据，如果已经存在则覆盖             |                                                                                                                                                                    |
    | list   | 字符串列表   | lpush | 在 list 最左边存储这个数据             |                                                                                                                                                                    |
    | list   | 字符串列表   | rpush | 在 list 最右边存储这个数据             |                                                                                                                                                                    |
    | set    | 字符串集合   | sadd  | 向 set 集合中存储这个数据，如果已经存在则覆盖    |                                                                                                                                                                    |
    | zset   | 有序字符串集合 | zadd  | 向 zset 有序集合中存储这个数据，如果已经存在则覆盖 | 当 value 类型是 zset 时，数据源的每一行记录需要遵循相应的规范，即每一行记录除 key 以外，只能有一对 score 和 value，并且 score 必须在 value 前面，rediswriter 方能解析出哪一个 column 是 score，哪一个 column 是 value。             |
    | hash   | 哈希      | hset  | 向 hash 有序集合中存储这个数据，如果已经存在则覆盖 | 当 value 类型是 hash 时，数据源的每一行记录需要遵循相应的规范，即每一行记录除 key 以外，只能有一对 attribute 和 value，并且 attribute 必须在 value 前面，Rediswriter 方能解析出哪一个 column 是 attribute，哪一个 column 是 value。 |
  
  * 必选：是
  
  * 默认值：无
