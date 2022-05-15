# HDFS Source

## Ⅰ、Introduction
The HDFS plugin supports reading and writing TextFile, Orc, and Parquet files directly from the configured HDFS path, and is generally used with Hive tables. For example: reading all data in a partition of the Hive table is essentially reading the data files under the HDFS path of the corresponding partition of the Hive table; writing data to a partition of the Hive table is essentially writing the data files directly to the HDFS of the corresponding partition Under the path; the HDFS plugin will not perform any DDL operations on the Hive table.
HDFS Source does not save the offset of the read file during checkpoint, so it does not support continued running.

## Ⅱ、Supported version
Hadoop 2.x、Hadoop 3.x


## Ⅲ、Plugin name
| Sync | hdfssource、hdfsreader |
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

- **defaultFS**
  - description：Hadoop hdfs file system namenode node address. Format: hdfs://ip:port; for example: hdfs://127.0.0.1:9000
  - required：required
  - type：String
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
        "index": 1,
        "isPart": false,
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

- **filterRegex**
  - description：File regular expression, read the matched file
  - required：optional
  - type：String
  - defaults：none
<br />

- **fieldDelimiter**
  - description: Field separator when fileType is text
  - required：optional
  - type：String
  - defaults：`\001`
<br />

- **encoding**
  - description：The character encoding of the field when fileType is text
  - required：optional
  - type：String
  - defaults：`UTF-8`


### 2、SQL
- **path**
  - description：The path of the data file to be read
  - required：required
  - type：String
  - defaults：none
    <br />

- **file-type**
  - description：○ File type, currently only supports user configuration as `text`, `orc`, `parquet`
    - text：textfile file format
    - orc：orcfile file format
    - parquet：parquet file format
  - required：required
  - type：String
  - defaults：none
    <br />

- **default-fs**
  - description：Hadoop hdfs file system namenode node address. Format: hdfs://ip:port; for example: hdfs://127.0.0.1:9000
  - required：required
  - type：String
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
        "index": 1,
        "isPart": false,
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
  ```text
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

- **filter-regex**
  - description：File regular expression, read the matched file
  - required：optional
  - type：String
  - defaults：none
    <br />

- **field-delimiter**
  - description: Field separator when fileType is text
  - required：optional
  - type：String
  - defaults：`\001`
    <br />

- **encoding**
  - description：The character encoding of the field when fileType is text
  - required：optional
  - type：String
  - defaults：`UTF-8`

- **scan.parallelism**
  - description：source parallelism
  - required：optional
  - type：String
  - defaults：none
    <br />

  
## Ⅴ、Data Type
| support | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY |
| --- | --- |
| Not supported yet | ARRAY、MAP、STRUCT、UNION |


## Ⅵ、Script example
See the `chunjun-examples` folder in the project.
