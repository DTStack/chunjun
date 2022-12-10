# Hbase Sink

## 一、介绍

hbase sink

## 二、支持版本

hbase1.4


## 三、插件名称

| Sync | hbasesink、hbasewriter |
| ---- |-----------------------|
| SQL  | hbase14-x             |


## 四、参数说明

### 1、Sync

- **table**
    - 描述：表名
    - 必选：是
    - 类型：String
    - 默认值：无


- **column**

    - 描述：要写入的hbase字段。
    - name：指定写入的hbase列，必须为 列族:列名 的格式；
    - type：指定源数据的类型，format指定日期类型的格式
    - 必选：是
    - 字段类型：List
    - 默认值：无
    - 注意：为了支持数据中某一列当做row_key这种情况，设置字符串rowkey为关键字，如果某一列name为rowkey，那么该列将不会写入


- **encoding**
    - 描述：编码
    - 必选：否
    - 类型：string
    - 默认值：utf-8


- **nullMode**
    - 描述：字段值为空时写入模式
    - 必选：否
    - 可选：SKIP：跳过，此字段不写入，EMPTY：空字节数组代替
    - 参数类型：string
    - 默认值：SKIP



- **walFlag**
    - 描述：是否跳过WAL
    - 必选：否
    - 参数类型：Boolean
    - 默认值：false
      <br />

- **writeBufferSize**
    - 描述：设置HBae client的写buffer大小，单位字节
    - 必选：否
    - 参数类型：Long
    - 默认值： 8 * 1024 * 1024
      <br />


- **batchSize**

    - 描述：一次性批量提交的记录数大小，该值可以极大减少ChunJun与数据库的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成ChunJun运行进程OOM情况
    - 必选：否
    - 参数类型：int
    - 默认值：1


- **rowkeyExpress**
    - 描述： 用于构造rowkey的描述信息，采用字符串格式，形式如下
      字符串格式为：$(cf:col)，可以多个字段组合：$(cf:col1)_$(cf:col2)，
      可以使用md5函数：md5($(cf:col))
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />



- **versionColumnIndex**
    - 描述：指定写入hbase的时间戳。支持：当前时间、指定时间列，指定时间，三者选一。若不配置表示用当前时间。index：指定对应reader端column的索引，从0开始，需保证能转换为long,若是Date类型，会尝试用yyyy-MM-dd HH:mm:ss和yyyy-MM-dd HH:mm:ss SSS去解析；若不指定index；value：指定时间的值,类型为字符串。
      注意，在hbase中查询默认会显示时间戳最大的数据，因此简单查询可能会出现看不到更新的情况，需要加过滤条件查询。
    - 必选：是
    - 参数类型：List
    - 默认值：无
      <br />

- **versionColumnValue**
    - 描述：目的表中的所有字段，字段之间用英文逗号分隔。例如: "column": ["id","name","age","hobby"]，如果不配置，将在系统表中获取
    - 必选：否
    - 参数类型：List
    - 默认值：无
      <br />


### 2、SQL

- **connector**
    - 描述：hbase14-x
    - 必选：是
    - 参数类型：String
    - 默认值：无

  
- **properties.zookeeper.znode.parent**
    - 描述：hbase在zk的路径
    - 必选：否
    - 参数类型：string
    - 默认值：/hbase
      <br />


- **properties.zookeeper.quorum**
    - 描述：zk地址
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



- **sink.buffer-flush.max-size**
    - 描述：每个写请求缓冲行的最大内存大小。这样可以提高HBase写数据的性能，但可能会增加时延。可以设置为'0'来禁用它。
    - 必选：否
    - 参数类型：String
    - 默认值：2mb
      <br />


- **sink.buffer-flush.max-rows**
    - 描述：每个写入请求要缓冲的最大行数。这样可以提高HBase写数据的性能，但可能会增加时延。可以设置为'0'来禁用它。
    - 必选：否
    - 参数类型：int
    - 默认值：1000
      <br />



- **sink.buffer-flush.interval**
    - 描述：批量写时间间隔，单位：毫秒
    - 必选：否
    - 参数类型：String
    - 默认值：10000
      <br />
    


- **sink.parallelism**
    - 描述：写入结果的并行度
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

    

## 五、数据类型


|     是否支持     |                    类型名称                    |
| :--------------: |:------------------------------------------:|
|       支持       | INT、DOUBLE、FLOAT、SHORT、BOOLEAN、STRING |
|     暂不支持     |                  |
| 仅在 Sync 中支持 |                         |

## 六、脚本示例

见项目内`chunjun-examples`文件夹。

