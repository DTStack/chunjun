# Redis Sink

## 一、介绍
redis sink

## 二、支持版本
主流版本


## 三、插件名称
| Sync | redissink、rediswriter |
| --- | --- |
| SQL | redis-x |


## 四、参数说明
### 1、Sync
- **hostPort**
  - 描述：Redis的IP地址和端口
  - 必选：是
  - 默认值：localhost:6379
<br />

- **password**
  - 描述：数据源指定用户名的密码
  - 必选：是
  - 默认值：无
<br />

- **database**
  - 描述：要写入Redis数据库
  - 必选：否
  - 默认值：0
<br />

- **keyFieldDelimiter**
  - 描述：写入 Redis 的 key 分隔符。比如: key=key1\u0001id，如果 key 有多个需要拼接时，该值为必填项，如果 key 只有一个则可以忽略该配置项。
  - 必选：否
  - 默认值：\u0001
<br />

- **dateFormat**
  - 描述：写入 Redis 时，Date 的时间格式：”yyyy-MM-dd HH:mm:ss”
  - 必选：否
  - 默认值：将日期以long类型写入
<br />

- **expireTime**
  - 描述：Redis value 值缓存失效时间（如果需要永久有效则可以不填该配置项）。
  - 注意：如果过期时间的秒数大于 60_60_24*30（即 30 天），则服务端认为是 Unix 时间，该时间指定了到未来某个时刻数据失效。否则为相对当前时间的秒数，该时间指定了从现在开始多长时间后数据失效。
  - 必选：否
  - 默认值：0（0 表示永久有效）
<br />

- **timeout**
  - 描述：写入 Redis 的超时时间。
  - 单位：毫秒
  - 必选：否
  - 默认值：30000
<br />

- **type和mode**
  - 描述：type 表示 value 的类型，mode 表示在选定的数据类型下的写入模式。
  - 选项：string/list/set/zset/hash

    | type | 描述 | mode | 说明 | 注意 |
    | ---- | ---- | ---- | ---- | ---- |
    | string | 字符串 | set | 存储这个数据，如果已经存在则覆盖 |  |
    | list | 字符串列表 | lpush | 在 list 最左边存储这个数据 |  |
    | list | 字符串列表 | rpush | 在 list 最右边存储这个数据 |  |
    | set | 字符串集合 | sadd | 向 set 集合中存储这个数据，如果已经存在则覆盖 |  |
    | zset | 有序字符串集合 | zadd | 向 zset 有序集合中存储这个数据，如果已经存在则覆盖 | 当 value 类型是 zset 时，数据源的每一行记录需要遵循相应的规范，即每一行记录除 key 以外，只能有一对 score 和 value，并且 score 必须在 value 前面，rediswriter 方能解析出哪一个 column 是 score，哪一个 column 是 value。 |
    | hash | 哈希 | hset | 向 hash 有序集合中存储这个数据，如果已经存在则覆盖 | 当 value 类型是 hash 时，数据源的每一行记录需要遵循相应的规范，即每一行记录除 key 以外，只能有一对 attribute 和 value，并且 attribute 必须在 value 前面，Rediswriter 方能解析出哪一个 column 是 attribute，哪一个 column 是 value。 |
  - 必选：是
  - 默认值：无
<br />

- **valueFieldDelimiter**
  - 描述：该配置项是考虑了当源数据每行超过两列的情况（如果您的源数据只有两列即 key 和 value 时，那么可以忽略该配置项，不用填写），value 类型是 string 时，value 之间的分隔符，比如 value1\u0001value2\u0001value3。
  - 必选：否
  - 默认值：\u0001
<br />

- **keyIndexes**
  - 描述：keyIndexes 表示源端哪几列需要作为 key（第一列是从 0 开始）。如果是第一列和第二列需要组合作为 key，那么 keyIndexes 的值则为 [0,1]。
  - 注意：配置 keyIndexes 后，Redis Writer 会将其余的列作为 value，如果您只想同步源表的某几列作为 key，某几列作为 value，不需要同步所有字段，那么在 Reader 插件端就指定好 column 作好列筛选即可。例如：Redis中的数据为 "test,redis,First,Second"，keyIndexes = [0,1] ，因此得到的key为 "test\\u0001redis"， value为 "First\\u0001Second"
  - 必选：是
  - 默认值：无
<br />

### 2、SQL
- **connector**
  - 描述：redis-x 
  - 必选：是
  - 参数类型：string
  - 默认值：无
<br />

- **url**
  - 描述：localhost:6379 
  - 必选：是
  - 参数类型：string
  - 默认值：无
<br />

- **table-name**
  - 描述：tableName 
  - 必选：是
  - 参数类型：string
  - 默认值：无
<br />

- **password**
  - 描述：password 
  - 必选：否
  - 参数类型：string
  - 默认值：无
<br />

- **redis-type**
  - 描述： redis模式（1 单机，2 哨兵， 3 集群），默认：1
  - 必选：否
  - 参数类型：string
  - 默认值：1
<br />

- **master-name**
  - 描述： 主节点名称（哨兵模式下为必填项）
  - 必选：否
  - 参数类型：string
  - 默认值：无
<br />

- **database**
  - 描述： redis 的数据库地址，默认：0
  - 必选：否
  - 参数类型：string
  - 默认值：0
<br />

- **timeout**
  - 描述：连接超时时间，默认：10000毫秒 
  - 必选：否
  - 参数类型：string
  - 默认值：10000
<br />

- **max.total**
  - 描述：最大连接数 ，默认：8 
  - 必选：否
  - 参数类型：string
  - 默认值：8
<br />

- **max.idle**
  - 描述：最大空闲连接数，默认：8 
  - 必选：否
  - 参数类型：string
  - 默认值：8
<br />

- **min.idle**
  - 描述： 最小空闲连接数 ，默认：0
  - 必选：否
  - 参数类型：string
  - 默认值：0
<br />

- **key.expired-time**
  - 描述：redis sink的key的过期时间。默认是0（永不过期），单位是s。默认：0
  - 必选：否
  - 参数类型：string
  - 默认值：0
<br />

- **sink.parallelism**
  - 描述：sink并行度 
  - 必选：否
  - 参数类型：string
  - 默认值：无
<br />

- **type和mode**
    - 描述：type 表示 value 的类型，mode 表示在选定的数据类型下的写入模式。
    - 选项：string/list/set/zset/hash

      | type | 描述 | mode | 说明 | 注意 |
      | ---- | ---- | ---- | ---- | ---- |
      | string | 字符串 | set | 存储这个数据，如果已经存在则覆盖 |  |
      | list | 字符串列表 | lpush | 在 list 最左边存储这个数据 |  |
      | list | 字符串列表 | rpush | 在 list 最右边存储这个数据 |  |
      | set | 字符串集合 | sadd | 向 set 集合中存储这个数据，如果已经存在则覆盖 |  |
      | zset | 有序字符串集合 | zadd | 向 zset 有序集合中存储这个数据，如果已经存在则覆盖 | 当 value 类型是 zset 时，数据源的每一行记录需要遵循相应的规范，即每一行记录除 key 以外，只能有一对 score 和 value，并且 score 必须在 value 前面，rediswriter 方能解析出哪一个 column 是 score，哪一个 column 是 value。 |
      | hash | 哈希 | hset | 向 hash 有序集合中存储这个数据，如果已经存在则覆盖 | 当 value 类型是 hash 时，数据源的每一行记录需要遵循相应的规范，即每一行记录除 key 以外，只能有一对 attribute 和 value，并且 attribute 必须在 value 前面，Rediswriter 方能解析出哪一个 column 是 attribute，哪一个 column 是 value。 |
    - 必选：是
    - 默认值：无
<br />
      

## 五、数据类型
| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |


## 六、脚本示例
见项目内`flinkx-examples`文件夹。
