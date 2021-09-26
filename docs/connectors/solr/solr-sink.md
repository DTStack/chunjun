## 一、介绍
Solr写入，目前只支持开启Kerberos的Solr数据源。


## 二、支持版本
Solr 7.4


## 三、插件名称
| Sync | solrsink、solrwriter |
| --- | --- |
| SQL | solr-x |


## 四、参数说明
### 1、Sync

- **zkHosts**
    - 描述：Solr的zookeeper集群地址，每个ZK节点为数组的一个元素
    - 必选：是
    - 参数类型：array
    - 默认值：无
    

- **zkChroot**
    - 描述：Solr所在的Zookeeper chroot
    - 必选：否
    - 参数类型：string
    - 默认值：无
    

- **collection**
    - 描述：Solr collection名称
    - 必选：是
    - 参数类型：string
    - 默认值：无


- **column**
    - 描述：需要读取的字段
    - 注意：不支持*格式
    - 格式：
```json
"column": [
  {
  	"name": "val_int",
  	"type": "long"
  }
]
```

- 必选：是
- 参数类型：数组
- 默认值：无

- **kerberosConfig**
    - 描述：开启kerberos时包含kerberos相关配置
    - 必选：否
    - 格式：
```json
"kerberosConfig": {
  "principal": "solr/worker@DTSTACK.COM",
  "keytab":"./solr.keytab",
  "krb5conf":"./krb5.conf"
}
```

- 参数类型：object
- 默认值：无
- **batchSize**
    - 描述：一次性批量提交的记录数大小，该值可以极大减少FlinkX与数据库的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成FlinkX运行进程OOM情况
    - 必选：否
    - 字段类型：int
    - 默认值：1
- **flushIntervalMills**
    - 描述：批量写入时间间隔：单位毫秒。
    - 必选：否
    - 字段类型：int
    - 默认值：10000

### 2、SQL

- **zk-hosts**
    - 描述：Solr的zookeeper集群地址，每个节点用, 分割
    - 必选：是
    - 参数类型：string
    - 默认值：无
- **zk-chroot**
    - 描述：Solr所在的Zookeeper chroot
    - 必选：否
    - 参数类型：string
    - 默认值：无
- **collection**
    - 描述：Solr collection名称
    - 必选：是
    - 参数类型：string
    - 默认值：无



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



- **sink.parallelism**
    - 描述：sink并行度
    - 必选：是
    - 默认值：无
- **sink.buffer-flush.max-rows**
    - 描述：批量写入条数
    - 必选：否
    - 默认值：无
- **sink.buffer-flush.interval**
    - 描述：批量写入时间间隔：单位毫秒。
    - 必选：否
    - 默认值：无



## 五、数据类型
| 支持 | bool |
| --- | --- |
|  | int |
|  | long |
|  | string |
|  | text |
|  | float |
|  | double |
|  | date |
| 暂不支持 | array |



## 六、脚本示例
见项目内`flinkx-examples`文件夹。
