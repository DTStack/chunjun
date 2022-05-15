# FileSystem Sink

## 一、介绍
filesystem sink 是文件写入系统的公共插件。 具体的的插件需要具体实现 ,用户只要配置不同的文件路径，系统
动态加载不同的文件系统插件，如hdfs:// s3a:// 等<br />

## 二、插件名称
| SQL | filesystem-x |
| --- |--------------|


## 三、参数说明
### 1、公共参数

- **path**
   - 描述：读取的数据文件路径
   - 必选：是
   - 参数类型：string
   - 默认值：无
<br />

- **format**
   - 描述：文件的类型，和原生flink保持一致，支持原生所有类型，详情见[flink官方文档](https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/table/connectors/filesystem.html#file-formats)
   - 必选：是
   - 参数类型：string
   - 默认值：无
<br />

- **sink.rolling-policy.file-size**
    - 描述：文件写入系统的大小
    - 必选：无
    - 参数类型：MemorySize
    - 默认值：128MB
<br />

- **sink.rolling-policy.rollover-interval**
    - 描述：文件写入系统的时间间隔
    - 必选：无
    - 参数类型：Duration
    - 默认值：30 min
 <br />

- **sink.rolling-policy.check-interval**
    - 描述：检查文件是否需要写入的定时任务启动间隔
    - 必选：无
    - 参数类型：Duration
    - 默认值：1 min
<br />

- **auto-compaction**
    - 描述：单次checkpoint产生的文件是否需要进行合并
    - 必选：无
    - 参数类型：boolean
    - 默认值：false
<br />

- **compaction.file-size**
    - 描述：单次checkpoint产生的文件合并的最大大小
    - 必选：无
    - 参数类型：MemorySize
    - 默认值：sink.rolling-policy.file-size
<br />

### 2、s3数据源参数

- **path**
    - 描述：此参数是公共参数，当数据源为s3 时，路径格式必须是s3a://{bucket}/{your path}
    - 必选：是
    - 参数类型：string
    - 默认值：无
 <br />

- **s3.access-key**
    - 描述：s3 SecretId
    - 必选：是
    - 参数类型：string
    - 默认值：无
<br />

- **s3.secret-key**
    - 描述：s3 SecretKey
    - 必选：是
    - 参数类型：string
    - 默认值：无
<br />

- **s3.endpoint**
    - 描述：s3.endpoint
    - 必选：是
    - 参数类型：string
    - 默认值：无
<br />

- **s3.path.style.access**
    - 描述：某些兼容 S3 的对象存储服务可能没有默认启用虚拟主机样式的寻址,此配置以启用路径样式的访问
    - 必选：否
    - 参数类型：boolean
    - 默认值：true
<br />

## 四、数据类型
和原生flink数据类型保持一致<br />每种format所支持的数据类型请参考[flink官方文档](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/formats/)<br />

## 五、脚本示例
见项目内`chunjun-examples`文件夹。

