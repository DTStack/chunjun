## 一、介绍
Solr读取，目前只支持开启Kerberos的Solr数据源。暂不支持SQL读。

## 二、支持版本
Solr 7.4

## 三、插件名称
| Sync | solrsource、solrreader |
| --- | --- |



## 四、参数说明
### 1、Sync

- **zkHosts**
    - 描述：Solr的zookeeper集群地址，每个ZK节点为数组的一个元素。
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
见项目内`chunjun-examples`文件夹。
