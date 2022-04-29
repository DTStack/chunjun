# Redis Lookup

## 一、Introduce

Redis dimension table supports full and async methods<br />
Full cache: Load all dimension table data into memory. It is recommended to use it with a small amount of data.<br />
Async cache: Query data asynchronously and cache the queried data into memory using LRU. It is recommended to use it with a large amount of data.

## 二、Supported Version

All major versions

## 三、Plugin Name

| SQL | redis-x |
| --- | --- |

## 四、Configuration

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

- **lookup.cache-type**
    - Description：Dimension table cache type (NONE、LRU、ALL)
    - Required：no
    - Type：string
    - Default：LRU
      <br />

- **lookup.cache-period**
    - Description：How often do all dimension tables load data
    - Unit：millisecond
    - Required：no
    - Type：string
    - Default：3600000
      <br />

- **lookup.cache.max-rows**
    - Description：Number of entries of LRU dimension table cache data
    - Required：no
    - Type：string
    - Default：10000
      <br />

- **lookup.cache.ttl**
    - Description：LRU dimension table cache data time
    - Unit：millisecond
    - Required：no
    - Type：string
    - Default：60000
      <br />

- **lookup.parallelism**
    - Description：parallelism
    - Required：no
    - Type：string
    - Default：(none)
      <br />

## 五、Data Types

| support | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| not support | ARRAY、MAP、STRUCT、UNION |

## 六、examples

link `flinkx-examples`
