# 数据源开启Kerberos安全认证

目前ChunJun的部分插件支持了kerberos认证，有Hive、Hbase、HDFS三个插件。

### 1.Kerberos证书加载方式

目前支持两种方式，一种是从本地加载，即任务运行的机器上对应的目录必须存在配置里指定的证书文件，另一种是从sftp服务器下载，需要配置sftp服务器的配置信息。

使用本地配置示例：

```json
"hbaseConfig": {
 "hbase.zookeeper.property.clientPort": "2181",
 "hbase.rootdir": "hdfs://ns1/hbase",
 "hbase.cluster.distributed": "true",
 "hbase.zookeeper.quorum": "host1,host2,host3",
 "zookeeper.znode.parent": "/hbase",
 "hbase.security.authentication":"Kerberos",
 "hbase.security.authorization":true,
 "hbase.master.kerberos.principal":"hbase/node1@TEST.COM",
 "hbase.master.keytab.file":"hbase.keytab",
 "hbase.regionserver.keytab.file":"hbase.keytab",
 "hbase.regionserver.kerberos.principal":"hbase/node1@TEST.COM",
 "java.security.krb5.conf":"krb5.conf",
 "useLocalFile":true
 }
```

从sftp下载配置示例：

```json
"hbaseConfig": {
 "hbase.zookeeper.property.clientPort": "2181",
 "hbase.rootdir": "hdfs://ns1/hbase",
 "hbase.cluster.distributed": "true",
 "hbase.zookeeper.quorum": "host1,host2,host3",
 "zookeeper.znode.parent": "/hbase",
 "hbase.security.authentication":"Kerberos",
 "hbase.security.authorization":true,
 "hbase.master.kerberos.principal":"hbase/node1@TEST.COM",
 "hbase.master.keytab.file":"hbase.keytab",
 "hbase.regionserver.keytab.file":"hbase.keytab",
 "hbase.regionserver.kerberos.principal":"hbase/node1@TEST.COM",
 "remoteDir":"/sftp/chunjun/keytab/hbase",
 "sftp":{
     "host":"127.0.0.1",
     "port":"22",
     "username":"",
     "password":""
 }
 }
```

从sftp下载时的查找顺序：

1.在/sftp/chunjun/keytab/hbase目录下查找hbase.keytab文件，如果找不到则2

2.假设任务运行在node1机器上，则在/sftp/chunjun/keytab/hbase/node1下找hbase.keytab文件，找不到则报错;

### 2.各数据源的配置

#### hbase

```json
"hbaseConfig": {
 "hbase.zookeeper.property.clientPort": "2181",
 "hbase.rootdir": "hdfs://ns1/hbase",
 "hbase.cluster.distributed": "true",
 "hbase.zookeeper.quorum": "host1,host2,host3",
 "zookeeper.znode.parent": "/hbase",
 "hbase.security.authentication":"Kerberos",
 "hbase.security.authorization":true,
 "hbase.master.kerberos.principal":"hbase/node1@TEST.COM",
 "hbase.master.keytab.file":"hbase.keytab",
 "hbase.regionserver.keytab.file":"hbase.keytab",
 "hbase.regionserver.kerberos.principal":"hbase/node1@TEST.COM",
 "java.security.krb5.conf":"krb5.conf"
 }
```

#### hive

```json
"hadoopConf":{
 "dfs.ha.namenodes.ns1": "nn1,nn2",
 "dfs.namenode.rpc-address.ns1.nn2": "node03:9000",
 "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
 "dfs.namenode.rpc-address.ns1.nn1": "node02:9000",
 "dfs.nameservices": "ns1"
 "hadoop.security.authorization": "true",
 "hadoop.security.authentication": "Kerberos",
 "dfs.namenode.kerberos.principal": "hdfs/_HOST@HADOOP.COM",
 "dfs.namenode.keytab.file": "hdfs.keytab",
 "java.security.krb5.conf": "krb5.conf"
}
```

jdbcUrl格式：jdbc:hive2://127.0.0.1:10000/default;principal=hive/node1@HADOOP.COM

#### hdfs

```json
"hadoopConf":{
    "dfs.ha.namenodes.ns1": "nn1,nn2",
 "dfs.namenode.rpc-address.ns1.nn2": "node03:9000",
 "dfs.client.failover.proxy.provider.ns1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
 "dfs.namenode.rpc-address.ns1.nn1": "node02:9000",
 "dfs.nameservices": "ns1"
    "hadoop.security.authorization": "true",
    "hadoop.security.authentication": "Kerberos",
    "dfs.namenode.kerberos.principal": "hdfs/_HOST@HADOOP.COM",
    "dfs.namenode.keytab.file": "hdfs.keytab",
    "java.security.krb5.conf": "krb5.conf"
}
```
