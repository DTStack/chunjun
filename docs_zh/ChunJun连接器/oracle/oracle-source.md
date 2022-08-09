# Oracle Source

## 一、介绍

支持从oracle离线读取

## 二、支持版本

Oracle 9 及以上


## 三、插件名称

| Sync | oraclesource、oraclereader |
| ---- | -------------------------- |
| SQL  | oracle-x                   |


## 四、参数说明

### 1、Sync

- **connection**

    - 描述：数据库连接参数，包含jdbcUrl、schema、table等参数

    - 必选：是

    - 参数类型：List

    - 默认值：无

      ```text
      "connection": [{
       "jdbcUrl": ["jdbc:oracle:thin:@0.0.0.1:1521:orcl"],
       "table": ["table"],
       "schema":"public"
      }]
      ```

       <br />

- **jdbcUrl**

    - 描述：针对关系型数据库的jdbc连接字符串,jdbcUrl参考文档：[Oracle官方文档](http://www.oracle.com/technetwork/database/enterprise-edition/documentation/index.html)
    - 必选：是
    - 参数类型：string
    - 默认值：用户名
      <br />

- **schema**

    - 描述：数据库schema名
    - 必选：否
    - 参数类型：string
    - 默认值：无
      <br />

- **table**

    - 描述：目的表的表名称。目前只支持配置单个表，后续会支持多表
    - 必选：是
    - 参数类型：List
    - 默认值：无
      <br />

- **username**

    - 描述：数据源的用户名
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **password**

    - 描述：数据源指定用户名的密码
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **fetchSize**

    - 描述：一次性从数据库中读取多少条数据，ORACLE默认fetchSize大小为10。当fetchSize设置过小时导致频繁读取数据会影响查询速度，以及数据库压力。当fetchSize设置过大时在数据量很大时可能会造成OOM，设置这个参数可以控制每次读取fetchSize条数据。
      注意：此参数的值不可设置过大，否则会读取超时，导致任务失败。
    - 必选：否
    - 参数类型：int
    - 默认值：1024
      <br />

- **where**

    - 描述：筛选条件，reader插件根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > time。
    - 注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **splitPk**

    - 描述：当speed配置中的channel大于1时指定此参数，Reader插件根据并发数和此参数指定的字段拼接sql，使每个并发读取不同的数据，提升读取速率。
    - 注意：
        - 推荐splitPk使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。
        - 目前splitPk仅支持整形数据切分，不支持浮点、字符串、日期等其他类型。如果用户指定其他非支持类型，ChunJun将报错。
        - 如果channel大于1但是没有配置此参数，任务将置为失败。
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **queryTimeOut**

    - 描述：查询超时时间，单位秒。
    - 注意：当数据量很大，或者从视图查询，或者自定义sql查询时，可通过此参数指定超时时间。
    - 必选：否
    - 参数类型：int
    - 默认值：1000
      <br />

- **customSql**

    - 描述：自定义的查询语句，如果只指定字段不能满足需求时，可通过此参数指定查询的sql，可以是任意复杂的查询语句。
    - 注意：
        - 只能是查询语句，否则会导致任务失败；
        - 查询语句返回的字段需要和column列表里的字段对应；
        - 当指定了此参数时，connection里指定的table无效；
        - 当指定此参数时，column必须指定具体字段信息，不能以*号代替；
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **column**

    - 描述：需要读取的字段。

    - 格式：支持3种格式
      <br />1.读取全部字段，如果字段数量很多，可以使用下面的写法：

      ```bash
      "column":["*"]
      ```

      2.只指定字段名称：

      ```
      "column":["id","name"]
      ```

      3.指定具体信息：

      ```json
      "column": [{
          "name": "col",
          "type": "datetime",
          "format": "yyyy-MM-dd hh:mm:ss",
          "value": "value"
      }]
      ```

    - 属性说明:

        - name：字段名称
        - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
        - format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
        - value：如果数据库里不存在指定的字段，则会把value的值作为常量列返回，如果指定的字段存在，当指定字段的值为null时，会以此value值作为默认值返回

    - 必选：是

    - 默认值：无
      <br />

- **polling**

    - 描述：是否开启间隔轮询，开启后会根据pollingInterval轮询间隔时间周期性的从数据库拉取数据。开启间隔轮询还需配置参数pollingInterval，increColumn，可以选择配置参数startLocation。若不配置参数startLocation，任务启动时将会从数据库中查询增量字段最大值作为轮询的起始位置。
    - 必选：否
    - 参数类型：Boolean
    - 默认值：false
      <br />

- **pollingInterval**

    - 描述：轮询间隔时间，从数据库中拉取数据的间隔时间，默认为5000毫秒。
    - 必选：否
    - 参数类型：long
    - 默认值：5000
      <br />

- **increColumn**

    - 描述：增量字段，可以是对应的增量字段名，也可以是纯数字，表示增量字段在column中的顺序位置（从0开始）
    - 必选：否
    - 参数类型：String或int
    - 默认值：无
      <br />

- **startLocation**

    - 描述：增量查询起始位置
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **useMaxFunc**

    - 描述：用于标记是否保存endLocation位置的一条或多条数据，true：不保存，false(默认)：保存， 某些情况下可能出现最后几条数据被重复记录的情况，可以将此参数配置为true
    - 必选：否
    - 参数类型：Boolean
    - 默认值：false
      <br />

- **requestAccumulatorInterval**

    - 描述：发送查询累加器请求的间隔时间。
    - 必选：否
    - 参数类型：int
    - 默认值：2
      <br />

### 2、SQL

- **connector**
    - 描述：oracle-x
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **url**
    - 描述：jdbc:oracle:thin:@0.0.0.1:1521:orcl
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />
  
- **schema**
    - 描述：数据库schema名
    - 必选：否
    - 参数类型：String
    - 默认值：用户名：
      <br />

- **table-name**
    - 描述：表名
    - 必选：是
    - 参数类型：String
    - 默认值：无：
      <br />

- **username**
    - 描述：username
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **password**
    - 描述：password
    - 必选：是
    - 参数类型：String
    - 默认值：无
      <br />

- **scan.polling-interval**
    - 描述：间隔轮训时间。非必填(不填为离线任务)，无默认
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **scan.parallelism**
    - 描述：并行度
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **scan.fetch-size**
    - 描述：每次从数据库中fetch大小，单位：条。
    - 必选：否
    - 参数类型：String
    - 默认值：1024
      <br />

- **scan.query-timeout**
    - 描述：数据库连接超时时间，单位：秒。
    - 必选：否
    - 参数类型：String
    - 默认值：1
      <br />

- **scan.partition.column**
    - 描述：多并行度读取的切分字段，多并行度下必需要设置
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **scan.partition.strategy**
    - 描述：数据分片策略
    - 必选：否
    - 参数类型：String
    - 默认值：range
      <br />

- **scan.increment.column**
    - 描述：增量字段名称
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **scan.increment.column-type**
    - 描述：增量字段类型
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **scan.start-location**
    - 描述：增量字段开始位置,如果不指定则先同步所有，然后在增量
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **scan.restore.columnname**
    - 描述：开启了cp，任务从sp/cp续跑字段名称。如果续跑，则会覆盖scan.start-location开始位置，从续跑点开始
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **scan.restore.columntype**
    - 描述：开启了cp，任务从sp/cp续跑字段类型
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

## 五、数据类型

|     是否支持      |                           类型名称                           |
|:-------------:| :----------------------------------------------------------: |
|      支持       | SMALLINT、BINARY_DOUBLE、CHAR、VARCHAR、VARCHAR2、NCHAR、NVARCHAR2、INT、INTEGER、NUMBER、DECIMAL、FLOAT、DATE、RAW、LONG RAW、BINARY_FLOAT、TIMESTAMP、TIMESTAMP WITH LOCAL TIME ZONE、TIMESTAMP WITH TIME ZON、INTERVAL YEAR、INTERVAL DAY |
|      不支持      |                 BFILE、XMLTYPE、Collections                  |
|  仅在 Sync 中支持  |                      BLOB、CLOB、NCLOB                       |


注意：由于 flink DecimalType 的 PRECISION(1~38) 与 SCALE(0~PRECISION) 限制，oracle 的数值类型的数据在转换时可能会丢失精度

## 六、脚本示例

见项目内`chunjun-examples`文件夹。
