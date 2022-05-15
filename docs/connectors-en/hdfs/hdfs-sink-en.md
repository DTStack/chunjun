# HDFS Sink

## Ⅰ、Introduction
The HDFS plugin supports reading and writing TextFile, Orc, and Parquet files directly from the configured HDFS path, and is generally used with Hive tables. For example: reading all data in a partition of the Hive table is essentially reading the data files under the HDFS path of the corresponding partition of the Hive table; writing data to a partition of the Hive table is essentially writing the data files directly to the HDFS of the corresponding partition Under the path; the HDFS plugin will not perform any DDL operations on the Hive table.
HDFS Sink will use two-phase commit when checkpoint is turned on. During pre-commit, the data files generated in the .data directory are copied to the official directory and the copied data files are marked. The data files marked in the .data directory are deleted during the commit phase and rolled back. Delete the data files marked in the official catalog at the time.


## Ⅱ、Supported version
Hadoop 2.x、Hadoop 3.x


## Ⅲ、Plugin name
| Sync | hdfssink、hdfswriter |
| --- | --- |
| SQL | hdfs-x |


## Ⅳ、Parameter Description

### 1、Sync
- **path**
  - description：The path of the data file to be written
  - notice：The file path actually written is path/fileName
  - required：required
  - type：string
  - defaults：none
  <br />

- **fileName**
  - description：Data file directory name
  - notice：The file path actually written is path/fileName
  - required：required
  - type：string
  - defaults：none
<br />

- **writeMode**
  - description：HDFS Sink data cleaning processing mode before writing：
    - append
    - overwrite
  - notice：All files in the current directory of hdfs will be deleted in overwrite mode
  - required：required
  - type：string
  - defaults：append
<br />

- **fileType**
  - description：○ File type, currently only supports user configuration as `text`, `orc`, `parquet`
    - text：textfile file format
    - orc：orcfile file format
    - parquet：parquet file format
  - required：required
  - type：string
  - defaults：none
<br />

- **defaultFS**
  - description：Hadoop hdfs file system namenode node address. Format: hdfs://ip:port; for example: hdfs://127.0.0.1:9000
  - required：required
  - type：string
  - defaults：none
<br />

- **column**
  - description：Need to read the field
  - notice：Does not support * format
  - format：
```text
"column": [{
    "name": "col",
    "type": "string",
    "index":1,
    "isPart":false,
    "format": "yyyy-MM-dd hh:mm:ss",
    "value": "value"
}]
```

- property description:
  - name：required，Field Name
  - type：required，Field type, which needs to match the actual field type in the data file
  - index：optional，The position index of the field in all fields, starting from 0, the default is -1, read in sequence in the order of the array, read the specified field column after configuration
  - isPart：optional，Whether it is a partition field, if it is a partition field, the partition assignment will be automatically intercepted from the path, the default is fale
  - format：optional，Format the date according to the specified format
  - value：optional，Constant field, return the value of value as a constant column
- required：required
- type：Array
- defaults：none
<br />

- **hadoopConfig**
  - description：The configuration in core-site.xml and hdfs-site.xml that need to be filled in the cluster HA mode, including kerberos related configuration when kerberos is turned on
  - required：optional
  - type：Map<String, Object>
  - defaults：none
<br />

- **fieldDelimiter**
  - description: Field separator when fileType is text
  - required：optional
  - type：string
  - defaults：`\001`
<br />

- **fullColumnName**
  - description：Field name written
  - required：optional
  - type：list
  - defaults：column name collection
<br />

- **fullColumnType**
  - description：Field type written
  - required：optional
  - type：list
  - defaults：column type collection
<br />

- **compress**
  - description：hdfs file compression type
    - text：Support `GZIP`, `BZIP2` format
    - orc：Support`SNAPPY`、`GZIP`、`BZIP`、`LZ4`format
    - parquet：Support`SNAPPY`、`GZIP`、`LZO`format
  - notice：`SNAPPY`format requires users to install SnappyCodec
  - required：optional
  - type：string
  - defaults：
    - text is not compressed by default
    - orc defaults to ZLIB format
    - parquet defaults to SNAPPY format
<br />

- **maxFileSize**
  - description：The maximum size of a single file written to hdfs, in bytes
  - required：optional
  - type：long
  - defaults：`1073741824`（1G）
<br />

- **nextCheckRows**
  - description：The number of intervals for checking the file size next time, and the file size of the current written file will be queried every time this number is reached
  - required：optional
  - type：long
  - defaults：`5000`
<br />

- **rowGroupSIze**
  - description：Set the size of the row group when the fileType is parquet, in bytes
  - required：optional
  - type：int
  - defaults：`134217728`（128M）
<br />

- **enableDictionary**
  - description：When fileType is parquet, whether to start dictionary encoding
  - required：optional
  - type：boolean
  - defaults：`true`
<br />

- **encoding**
  - description：The character encoding of the field when fileType is text
  - required：optional
  - type：string
  - defaults：`UTF-8`

    
### 2、SQL
- **path**
  - description：The path of the data file to be written
  - notice：The file path actually written is path/fileName
  - required：required
  - type：string
  - defaults：none
    <br />

- **file-name**
  - description：Data file directory name
  - notice：The file path actually written is path/fileName
  - required：required
  - type：string
  - defaults：none
    <br />

- **write-mode**
  - description：HDFS Sink data cleaning processing mode before writing：
    - append
    - overwrite
  - notice：All files in the current directory of hdfs will be deleted in overwrite mode
  - required：required
  - type：string
  - defaults：append
    <br />

- **file-type**
  - description：○ File type, currently only supports user configuration as `text`, `orc`, `parquet`
    - text：textfile file format
    - orc：orcfile file format
    - parquet：parquet file format
  - required：required
  - type：string
  - defaults：none
    <br />

- **default-fs**
  - description：Hadoop hdfs file system namenode node address. Format: hdfs://ip:port; for example: hdfs://127.0.0.1:9000
  - required：required
  - type：string
  - defaults：none
    <br />

- **column**
  - description：Need to read the field
  - notice：Does not support * format
  - format：
```text
"column": [{
    "name": "col",
    "type": "string",
    "index":1,
    "isPart":false,
    "format": "yyyy-MM-dd hh:mm:ss",
    "value": "value"
}]
```

- property description:
  - name：required，Field Name
  - type：required，Field type, which needs to match the actual field type in the data file
  - index：optional，The position index of the field in all fields, starting from 0, the default is -1, read in sequence in the order of the array, read the specified field column after configuration
  - isPart：optional，Whether it is a partition field, if it is a partition field, the partition assignment will be automatically intercepted from the path, the default is fale
  - format：optional，Format the date according to the specified format
  - value：optional，Constant field, return the value of value as a constant column
- required：required
- type：Array
- defaults：none
  <br />

- **hadoopConfig**
  - description：The configuration in core-site.xml and hdfs-site.xml that need to be filled in the cluster HA mode, including kerberos related configuration when kerberos is turned on
  - required：optional
  - defaults：none
  - configuration method：'properties.key' ='value', key is the key in hadoopConfig, and value is the value in hadoopConfig, as shown below:
```
'properties.hadoop.user.name' = 'root',
'properties.dfs.ha.namenodes.ns' = 'nn1,nn2',
'properties.fs.defaultFS' = 'hdfs://ns',
'properties.dfs.namenode.rpc-address.ns.nn2' = 'ip:9000',
'properties.dfs.client.failover.proxy.provider.ns' = 'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
'properties.dfs.namenode.rpc-address.ns.nn1' = 'ip:9000',
'properties.dfs.nameservices' = 'ns',
'properties.fs.hdfs.impl.disable.cache' = 'true',
'properties.fs.hdfs.impl' = 'org.apache.hadoop.hdfs.DistributedFileSystem'
```

- **field-delimiter**
  - description: Field separator when fileType is text
  - required：optional
  - type：string
  - defaults：`\001`
    <br />

- **compress**
  - description：hdfs file compression type
    - text：Support `GZIP`, `BZIP2` format
    - orc：Support`SNAPPY`、`GZIP`、`BZIP`、`LZ4`format
    - parquet：Support`SNAPPY`、`GZIP`、`LZO`format
  - notice：`SNAPPY`format requires users to install SnappyCodec
  - required：optional
  - type：string
  - defaults：
    - text is not compressed by default
    - orc defaults to ZLIB format
    - parquet defaults to SNAPPY format
      <br />

- **max-file-size**
  - description：The maximum size of a single file written to hdfs, in bytes
  - required：optional
  - type：long
  - defaults：`1073741824`（1G）
    <br />

- **next-check-rows**
  - description：The number of intervals for checking the file size next time, and the file size of the current written file will be queried every time this number is reached
  - required：optional
  - type：long
  - defaults：`5000`
    <br />

- **enable-dictionary**
  - description：When fileType is parquet, whether to start dictionary encoding
  - required：optional
  - type：boolean
  - defaults：`true`
    <br />

- **encoding**
  - description：The character encoding of the field when fileType is text
  - required：optional
  - type：string
  - defaults：`UTF-8`

- **sink.parallelism**
  - description：sink parallelism
  - required：optional
  - type：string
  - defaults：none
    <br />


## Ⅴ、Data Type
| Support | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| Not supported yet | ARRAY、MAP、STRUCT、UNION |


## Ⅵ、Script example
See the `chunjun-examples` folder in the project.

