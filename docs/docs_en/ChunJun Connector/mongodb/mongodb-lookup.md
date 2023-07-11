## 一、介绍
MongoDB Lookup查询数据。

## 二、支持版本
MongoDB 3.4及以上

## 三、插件名称
| SQL | mongodb-x |
| --- | --- |



## 四、参数说明

- **connector**
    - 描述：mongodb-x
    - 必选：是
    - 默认值：无



- **url**
    - 描述：mongodb://xxxx
    - 必选：是
    - 默认值：无



- **collection**
    - 描述：集合名
    - 必选：是
    - 默认值：无



- **username**
    - 描述：用户名
    - 必选：是
    - 默认值：无



- **password**
    - 描述：密码
    - 必选：否
    - 默认值：无



- **lookup.cache-type**
    - 描述：维表缓存类型(NONE、LRU、ALL)，默认LRU
    - 必选：否
    - 默认值：LRU



- **lookup.cache-period**
    - 描述：ALL维表每隔多久加载一次数据，默认3600000毫秒(一个小时)
    - 必选：否
    - 默认值：3600000



- **lookup.cache.max-rows**
    - 描述：lru维表缓存数据的条数
    - 必选：否
    - 默认值：10000



- **lookup.cache.ttl**
    - 描述：lru维表缓存数据的时间
    - 必选：否
    - 默认值：60000



- **lookup.fetch-size**
    - 描述：ALL维表每次从数据库加载的条数
    - 必选：否
    - 默认值：1000



- **lookup.parallelism**
    - 描述：维表并行度
    - 必选：否
    - 默认值：无
  
## 五、数据类型
| 是否支持 | 类型名称 |
| --- | --- |
| 支持 | long  double  decimal objectId string bindata date timestamp bool |
| 不支持 | array |

## 六、脚本示例
见项目内`chunjun-examples`文件夹。


