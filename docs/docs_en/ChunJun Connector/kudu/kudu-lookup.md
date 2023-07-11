# Kudu Lookup

## 一、介绍

Kudu维表，支持全量和异步方式
全量缓存:将维表数据全部加载到内存中，建议数据量不大，且数据不经常变动的场景使用。
异步缓存:使用异步方式查询数据，并将查询到的数据使用lru缓存到内存中，建议数据量大使用。

## 二、支持版本

kudu 常用版本

## 三、插件名称

| SQL | kudu-x |
| --- | --- |

## 四、参数说明

- **masters**
    - 描述：kudu的IP地址和端口
    - 必选：是
    - 参数类型：string
    - 默认值：无
      

- **table-name**
    - 描述：要写入kudu表名
    - 必选：是
    - 参数类型：string
    - 默认值：无
      

- **client.worker-count**
    - 描述：kudu worker的数量
    - 必选：否
    - 参数类型：int
    - 默认值：2
      

- **client.default-operation-timeout-ms**
    - 描述：kudu operation超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      

- **client.default-admin-operation-timeout-ms**
    - 描述：admin operation 的超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      

- **scan-token.query-timeout**
    - 描述：query operation 的超时时间
    - 必选：否
    - 参数类型：int
    - 默认值：30 * 1000（30秒）
      

- **lookup.cache-type**
    - 描述：维表缓存类型(NONE、LRU、ALL)，默认LRU
    - 必选：否
    - 参数类型：string
    - 默认值：LRU
      

- **lookup.cache-period**
    - 描述：ALL维表每隔多久加载一次数据，默认3600000毫秒(一个小时)
    - 必选：否
    - 参数类型：string
    - 默认值：3600000
      

- **lookup.cache.max-rows**
    - 描述：lru维表缓存数据的条数，默认10000条
    - 必选：否
    - 参数类型：string
    - 默认值：10000
      

- **lookup.cache.ttl**
    - 描述：lru维表缓存数据的时间，默认60000毫秒(一分钟)
    - 必选：否
    - 参数类型：string
    - 默认值：60000


- **lookup.parallelism**
  - 描述：维表并行度
  - 必选：否
  - 参数类型：string
  - 默认值：无
    

- **scanner.batch-size-bytes**
    - 描述：scanner在每批中返回的最大字节数
    - 必选：否
    - 参数类型：string
    - 默认值：0
  

- **scanner.limit**
    - 描述：scanner将返回的行数限制
    - 必选：否
    - 参数类型：string
    - 默认值：1000 
  

- **scanner.fault-tolerant**
  - 描述：如果为真，如果当前服务器失败，则在另一台tablet服务器上恢复扫描
  - 必选：否
  - 参数类型：string
  - 默认值：无


## 五、数据类型

|是否支持 | 类型名称 |
| --- | --- |
| 支持 | INT8、BYTE、INT16、SHORT、INT32、INT、INT64、LONG、FLOAT、DOUBLE、BOOLEAN、STRING、VARCHAR、DECIMAL、TIMESTAMP、DATE、TIME、BINARY |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |

## 六、脚本示例

见项目内`chunjun-examples`文件夹。
