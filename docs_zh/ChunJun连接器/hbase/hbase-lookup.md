## 一、介绍
Hbase Lookup查询数据。

## 二、支持版本
hbase1.4

## 三、插件名称
| SQL | hbase14-x |
| --- |---------|



## 四、参数说明

- **connector**
    - 描述：hbase14-x
    - 必选：是
    - 默认值：无
    

- **properties.zookeeper.quorum**
    - 描述：zk地址
    - 必选：是
    - 示例：xxx:2181,xxx:2181,xxx:2181
    - 默认值：无


- **properties.zookeeper.znode.parent**
  - 描述：hbase在zk目录
  - 必选：否
  - 默认值：/hbase

  
- **null-string-literal**
    - 描述：空值字符串代替
    - 必选：否
    - 默认值："null"


- **lookup.cache-type**
    - 描述：维表缓存类型(NONE、LRU、ALL)，默认LRU
    - 必选：否
    - 默认值：LRU



- **lookup.error-limit**
    - 描述：异步维表加载数据错误数量上限
    - 必选：LRU异步
    - 默认值：Long.MAX_VALUE


- **lookup.cache-period**
  - 描述：ALL维表每隔多久加载一次数据，默认3600000毫秒(一个小时)
  - 必选：否
  - 默认值：3600000


- **lookup.async-timeout**
  - 描述：维表查询超时时间
  - 必选：否
  - 默认值：10000


- **lookup.cache.max-rows**
    - 描述：lru维表缓存数据的条数
    - 必选：否
    - 默认值：10000
    

- **lookup.cache.ttl**
    - 描述：lru维表缓存数据的时间
    - 必选：否
    - 默认值：60000


- **lookup.parallelism**
    - 描述：维表并行度
    - 必选：否
    - 默认值：无
## 
## 五、脚本示例
见项目内`chunjun-examples`文件夹。


