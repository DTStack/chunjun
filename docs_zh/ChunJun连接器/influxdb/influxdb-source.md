# InfluxDB Source

## 一、介绍

支持从influxDB离线读取,不支持断点续传（只能根据 time 排序，而 time 非主键，不唯一）

## 二、支持版本

influxDB 1.x

## 三、插件名称

| Sync | influxdbsource、influxdbreader |
| ---- |-------------------------------|
| SQL  |                         |


## 四、参数说明

### 1、Sync

- **connection**

    - 描述：数据库连接参数，包含jdbcUrl、database、measurement等参数

    - 必选：是

    - 参数类型：List

    - 默认值：无

      ```text
      "connection": [{
       "url": ["http://127.0.0.1:8086"],
       "measurement": ["measurement"],
       "database":"public"
      }]
      ```

       <br />

- **url**

    - 描述：连接influxDB的url
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />
  
- **database**

    - 描述：数据库名
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **measurement**

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

- **format**
    - 描述：响应格式
    - 必选：否
    - 参数类型：string
    - 默认值：MSGPACK
    - 可选值：MSGPACK/JSON
        - 区别：<br/>
          ⅰ. JSON 无法区分浮点数和整数<br/>
          ⅱ. JSON 不支持大于 2^53 的整数<br/>
          ⅲ. JSON 具有有限的性能特征<br/>
          ⅳ. 当 format 为 json 时，time 字段会在反序列化中 double 类型强转 long
          时丢失精度，详情见 https://github.com/influxdata/influxdb-java/issues/517
          <br />
        
- **fetchSize**

    - 描述：一次性从数据库中读取多少条数据。当fetchSize设置过小时导致频繁读取数据会影响查询速度，以及数据库压力。当fetchSize设置过大时在数据量很大时可能会造成OOM，设置这个参数可以控制每次读取fetchSize条数据。
      注意：此参数的值不可设置过大，否则会读取超时，导致任务失败。
    - 必选：否
    - 参数类型：int
    - 默认值：1000
      <br />

- **where**

    - 描述：描述：筛选条件，reader插件根据指定的column、table、where条件拼接 InfluxQL，并根据这个 InfluxQL 进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为time > currenttime
    - 注意：需符合 InfluxQL 语法,不要添加 order by
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **splitPk**

    - 描述：当speed配置中的channel大于1时指定此参数，Reader插件根据并发数和此参数指定的字段拼接 InfluxQL ，使每个并发读取不同的数据，提升读取速率。
    - 注意：
        - 不支持 tags 为切分主键，因为 tags 的类型只能为字符串
        - 不支持 time 为切分主键，因为 time 字段无法参与数学计算
        - 目前splitPk仅支持整形数据切分，不支持浮点、字符串、日期等其他类型。如果用户指定其他非支持类型，ChunJun将报错！
        - 如果channel大于1但是没有配置此参数，任务将置为失败。
    - 必选：否
    - 参数类型：String
    - 默认值：无
      <br />

- **epoch**

    - 描述：返回的time精度
    - 注意：当 format 为 json 时，time 字段会在反序列化中 double 类型强转 long 时丢失精度，详情见 https://github.com/influxdata/influxdb-java/issues/517
    - 可选值：h、m、s、ms、u、n
    - 必选：否
    - 参数类型：String
    - 默认值：n
      <br />

- **queryTimeOut**

    - 描述：查询超时时间，单位秒。
    - 必选：否
    - 参数类型：int
    - 默认值：3
      <br />

- **customSql**

    - 描述：自定义的查询语句，如果只指定字段不能满足需求时，可通过此参数指定查询的sql，可以是任意复杂的查询语句。
    - 注意：
        - 只能是查询语句，否则会导致任务失败；
        - 查询语句返回的字段需要和column列表里的字段对应；
        - 当指定了此参数时，connection里指定的table无效；
        - 当指定此参数时，column必须指定具体字段信息，不能以*号代替；
        - 禁止将time字段以外的字段作为order by字段
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
    


## 五、数据类型

|     是否支持     |                           类型名称                           |
|:---------------:| :----------------------------------------------------------: |
|       支持       | SMALLINT、BINARY_DOUBLE、CHAR、VARCHAR、VARCHAR2、NCHAR、NVARCHAR2、INT、INTEGER、NUMBER、DECIMAL、FLOAT、DATE、RAW、LONG RAW、BINARY_FLOAT、TIMESTAMP、TIMESTAMP WITH LOCAL TIME ZONE、TIMESTAMP WITH TIME ZON、INTERVAL YEAR、INTERVAL DAY |


## 六、脚本示例

见项目内`chunjun-examples`文件夹。
