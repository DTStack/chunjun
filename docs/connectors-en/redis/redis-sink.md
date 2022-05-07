# Redis Sink

## 一、Introduce
Redis sink

## 二、Supported Version
All major versions


## 三、Plugin Name
| Sync | redissink、rediswriter |
| --- | --- |
| SQL | redis-x |


## 四、Configuration
### 1、Sync
- **hostPort**
  - Description：The Redis address and port
  - Required：yes
  - Default：localhost:6379
<br />

- **password**
  - Description：The Redis password
  - Required：yes
  - Default：(none)
<br />

- **database**
  - Description：The Redis database
  - Required：no
  - Default：0
<br />

- **keyFieldDelimiter**
  - Description：Write the key separator of redis. If multiple keys need to be spliced, this value is required. If there is only one key, this configuration item can be ignored. For example: key = key1 \ u0001id
  - Required：no
  - Default：\u0001
<br />

- **dateFormat**
  - Description：When writing to redis, the time format of date
  - Required：no
  - Default：(none)
<br />

- **expireTime**
  - Description：Redis value cache expiration time (if it needs to be permanently valid, this configuration item can be omitted)
  - Required：no
  - Default：0
<br />

- **timeout**
  - Description：Timeout for writing to redis
  - Unit：millisecond
  - Required：no
  - Default：30000
<br />

- **type和mode**
  - Description：Type indicates the data type and mode indicates the write mode
  - Options：string/list/set/zset/hash

    | Type | Mode | 
    | ---- | ---- |
    | string | set | 
    | list | lpush | 
    | list | rpush | 
    | set |  sadd | 
    | zset | zadd |  
    | hash | hset |
  - Required：yes
  - Default：(none)
<br />

- **valueFieldDelimiter**
  - Description：This configuration item considers that when each row of the source data exceeds two columns (if your source data has only two columns, namely key and value, you can ignore this configuration item and do not fill it in). When the value type is string, the separator between values, such as value1 \ u0001value2 \ u0001value3
  - Required：no
  - Default：\u0001
<br />

- **keyIndexes**
  - Description：Keyindexes indicates which columns on the source side need to be used as keys (the first column starts from 0). If the first and second columns need to be combined as keys, the value of keyindexes is [0,1]
  - Note：After configuring keyindexes, redis writer will take the remaining columns as values. If you only want to synchronize some columns of the source table as keys and some columns as values, you don't need to synchronize all fields, you can specify columns on the reader plug-in side and filter the columns. For example, the data in redis is "test, redis, first, second", keyindexes = [0,1], so the obtained key is "test \ \ u0001redis", and the value is "first \ \ u0001second"
  - Required：yes
  - Default：(none)
<br />

### 2、SQL
- **connector**
  - Description：redis-x 
  - Required：yes
  - Type：string
  - Default：(none)
<br />

- **url**
  - Description：localhost:6379 
  - Required：yes
  - Type：string
  - Default：(none)
<br />

- **table-name**
  - Description：tableName 
  - Required：yes
  - Type：string
  - Default：(none)
<br />

- **password**
  - Description：password 
  - Required：no
  - Type：string
  - Default：(none)
<br />

- **redis-type**
  - Description： Redis mode（1 standalone，2 sentinel， 3 cluster）
  - Required：no
  - Type：string
  - Default：1
<br />

- **master-name**
  - Description： Master node name (required in sentinel mode)
  - Required：no
  - Type：string
  - Default：(none)
<br />

- **database**
  - Description： Redis database index
  - Required：no
  - Type：string
  - Default：0
<br />

- **timeout**
  - Description：Connection timeout 
  - Unit：millisecond
  - Required：no
  - Type：string
  - Default：10000
<br />

- **max.total**
  - Description：Maximum connection
  - Required：no
  - Type：string
  - Default：8
<br />

- **max.idle**
  - Description：Maximum number of free connections
  - Required：no
  - Type：string
  - Default：8
<br />

- **min.idle**
  - Description： Minimum number of free connections
  - Required：no
  - Type：string
  - Default：0
<br />

- **key.expired-time**
  - Description：The expiration time of the key of redis sink. The default is 0 (never expires)
  - Unit：second
  - Required：no
  - Type：string
  - Default：0
<br />

- **sink.parallelism**
  - Description：Sink parallelism 
  - Required：no
  - Type：string
  - Default：(none)
<br />

- **type和mode**
    - Description：Type indicates the data type and mode indicates the write mode
    - Options：string/list/set/zset/hash

      | Type | Mode | 
      | ---- | ---- |
      | string | set | 
      | list | lpush | 
      | list | rpush | 
      | set |  sadd | 
      | zset | zadd |  
      | hash | hset |
    - Required：yes
    - Default：(none)
<br />
      

## 五、Data Types
| support   | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| ---------- | --- |
| not support | ARRAY、MAP、STRUCT、UNION |


## 六、examples
link `flinkx-examples`
