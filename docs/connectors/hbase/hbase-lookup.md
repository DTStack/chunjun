# HBase Lookup

## 一、介绍
HBase维表，支持全量和异步方式<br />
全量缓存:将维表数据全部加载到内存中，建议数据量不大，且数据不经常变动的场景使用。<br />
异步缓存:使用异步方式查询数据，并将查询到的数据使用lru缓存到内存中，建议数据量大使用。

## 二、支持版本
HBase 1.4 +


## 三、插件名称
| SQL | hbase14-x |
| --- | --- |

## 四、参数说明
- **connector**
    - 描述：hbase14-x
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />
- **table-name**
    - 描述：表名
    - 必选：是
    - 参数类型：String
    - 默认值：无：
      <br />
- **zookeeper.quorum**
    - 描述：HBase的Zookeeper地址
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />
- **zookeeper.znode.parent**
    - 描述：root dir
    - 必选：是
    - 参数类型：String
    - 默认值：/hbase
      <br />

- **null-string-literal**
    - 描述：当字符串值为 null 时的存储形式
    - 必选：是
    - 参数类型：String
    - 默认值：null
      <br />
- **properties.***
    - 描述：HBase原生选项 如'properties.hbase.security.authentication' = 'kerberos'.
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />
      
- **lookup.max-retries**
  - 描述：the max retry times if lookup database failed.
  - 必选：否
  - 参数类型：int
  - 默认值：3
<br />

- **lookup.async-timeout**
    - 描述：async timeout.
    - 必选：否
    - 参数类型：int
    - 默认值：10000
      <br />

- **lookup.error-limit**
  - 描述：errorLimit
  - 必选：是
  - 参数类型：Long.MAX_VALUE
  - 默认值：无
<br />

- **lookup.cache-type**
  - 描述：维表缓存类型(NONE、LRU、ALL)，默认LRU
  - 必选：否
  - 参数类型：string
  - 默认值：LRU
<br />

- **lookup.cache-period**
  - 描述：ALL维表每隔多久加载一次数据，默认3600000毫秒(一个小时)
  - 必选：否
  - 参数类型：string
  - 默认值：3600000
<br />

- **lookup.cache.max-rows**
  - 描述：lru维表缓存数据的条数，默认10000条
  - 必选：否
  - 参数类型：string
  - 默认值：10000
<br />

- **lookup.cache.ttl**
  - 描述：lru维表缓存数据的时间，默认60000毫秒(一分钟)
  - 必选：否
  - 参数类型：string
  - 默认值：60000
<br />

- **lookup.fetch-size**
  - 描述：ALL维表每次从数据库加载的条数，默认1000条
  - 必选：否
  - 参数类型：string
  - 默认值：1000
<br />

- **lookup.parallelism**
  - 描述：维表并行度
  - 必选：否
  - 参数类型：string
  - 默认值：无
<br />
    
- **security.kerberos.principal**
    - 描述：kerberos认证的principal
    - 必选：是
    - 默认值：无
- **security.kerberos.keytab**
    - 描述：kerberos认证的keytab文件路径
    - 必选：是
    - 默认值：无
- **security.kerberos.krb5conf**
    - 描述：kerberos认证的krb5conf文件路径
    - 必选：是
    - 默认值：无
    
## 五、数据类型
| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| 暂不支持 | ARRAY、MAP、STRUCT、UNION |


## 六、脚本示例
见项目内`flinkx-examples`文件夹。
