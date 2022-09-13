# Sybase Source

## 一、介绍

支持从 sybase离线读取，支持 sybase实时间隔轮询读取

## 二、支持版本

sybase 15.7

## 三、插件名称

| 插件模式 | 插件名称     |
| -------- | ------------ |
| sync     | sybasereader |

## 四、参数说明

### 1、Sync

- **connection**

  - 描述：数据库连接参数，包含 jdbcUrl、schema、table 等参数
  - 必选：是
  - 参数类型：List
  - 默认值：无
    ```json
    "connection": [{
     "jdbcUrl": ["jdbc:jtds:sybase://hostname:port/database"],
     "table": ["table"],
     "schema":"public"
    }]
    ```
    <br />

- **jdbcUrl**

  - 描述：针对关系型数据库的 jdbc 连接字符串,jdbcUrl。
  - 必选：是
  - 参数类型：string
  - 默认值：无
    <br />

- **schema**

  - 描述：数据库 schema 名
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

  - 描述：一次性从数据库中读取多少条数据，Sybase 默认一次将所有结果都读取到内存中，在数据量很大时可能会造成 OOM，设置这个参数可以控制每次读取 fetchSize 条数据，而不是默认的把所有数据一次读取出来；开启 fetchSize 需要满足：连接参数 useCursorFetch=true。
    注意：此参数的值不可设置过大，否则会读取超时，导致任务失败。
  - 必选：否
  - 参数类型：int
  - 默认值：1024
    <br />

- **where**

  - 描述：筛选条件，reader 插件根据指定的 column、table、where 条件拼接 SQL，并根据这个 SQL 进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将 where 条件指定为 gmt_create > time。
  - 注意：不可以将 where 条件指定为 limit 10，limit 不是 SQL 的合法 where 子句。
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />

- **splitPk**

  - 描述：当 speed 配置中的 channel 大于 1 时指定此参数，Reader 插件根据并发数和此参数指定的字段拼接 sql，使每个并发读取不同的数据，提升读取速率。
  - 注意：
    - 推荐 splitPk 使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。
    - 目前 splitPk 仅支持整形数据切分，不支持浮点、字符串、日期等其他类型。如果用户指定其他非支持类型，chunjun 将报错。
    - 如果 channel 大于 1 但是没有配置此参数，任务将置为失败。
    - 仅支持数值型
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />

- **splitStrategy**

  - 描述：分片策略，当speed 配置中的 channel 大于 1 时此参数才生效
  - 所有选项：
    - range
      - 分片时获取splitPk在表中的最大值和最小值之差，尽可能均匀地分配给各个分片
        - splitPk=id，最大值=1，最小值=7，channel=3
          - channel-0：id >= 1 and id < 3
          - channel-1: id >= 3 and id < 5
          - channel-2: id >= 5
    - mod
      - 分片时根据分片数量对splitPk做mod操作
        - splitPk=id,chaeenl=3
          - channel-0：id mod 3 = 0
          - channel-1: id mod 3 = 1
          - channel-2: id mod 3 = 2
  - 注意:
    - 目前增量模式下仅支持mod
  - 必选：否
  - 参数类型：String
  - 默认值：
    - 增量模式：mode
    - 其他：   range
    <br />

- **queryTimeOut**

  - 描述：查询超时时间，单位秒。
  - 注意：当数据量很大，或者从视图查询，或者自定义 sql 查询时，可通过此参数指定超时时间。
  - 必选：否
  - 参数类型：int
  - 默认值：1000
    <br />

- **customSql**

  - 描述：自定义的查询语句，如果只指定字段不能满足需求时，可通过此参数指定查询的 sql，可以是任意复杂的查询语句。
  - 注意：
    - 只能是查询语句，否则会导致任务失败；
    - 查询语句返回的字段需要和 column 列表里的字段对应；
    - 当指定了此参数时，connection 里指定的 table 无效；
    - 当指定此参数时，column 必须指定具体字段信息，不能以\*号代替；
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />

- **column**

  - 描述：需要读取的字段。
  - 格式：支持 3 种格式
    <br />1. 读取全部字段，如果字段数量很多，可以使用下面的写法：
    ```bash
    "column":["*"]
    ```
    2. 只指定字段名称：
    ```
    "column":["id","name"]
    ```
    3. 指定具体信息：

  ```JSON
    "column": {
      "name": "col",
      "type": "datetime",
      "format": "yyyy-MM-dd hh:mm:ss",
      "value": "value"
    }
  ```

  - 属性说明:
    - name：字段名称
    - type：字段类型，可以和数据库里的字段类型不一样，程序会做一次类型转换
    - format：如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回
    - value：如果数据库里不存在指定的字段，则会把 value 的值作为常量列返回，如果指定的字段存在，当指定字段的值为 null 时，会以此 value 值作为默认值返回
  - 必选：是
  - 默认值：无
    <br />

- **polling**

  - 描述：是否开启间隔轮询，开启后会根据 pollingInterval 轮询间隔时间周期性的从数据库拉取数据。开启间隔轮询还需配置参数 pollingInterval，increColumn，可以选择配置参数 startLocation。若不配置参数 startLocation，任务启动时将会从数据库中查询增量字段最大值作为轮询的起始位置。
  - 必选：否
  - 参数类型：Boolean
  - 默认值：false
    <br />

- **pollingInterval**

  - 描述：轮询间隔时间，从数据库中拉取数据的间隔时间，默认为 5000 毫秒。
  - 必选：否
  - 参数类型：long
  - 默认值：5000
    <br />

- **increColumn**

  - 描述：增量字段，可以是对应的增量字段名，也可以是纯数字，表示增量字段在 column 中的顺序位置（从 0 开始）
  - 必选：否
  - 参数类型：String 或 int
  - 默认值：无
    <br />

- **orderByColumn**

  - 描述：排序字段，用于拼接sql语句中的order by语句
  - 必选：否
  - 参数类型：String
  - 注意：在增量模式中不生效，增量模式始终使用increColumn做order by
  - 默认值：无
    <br />
  
- **startLocation**

  - 描述：增量查询起始位置
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />

- **useMaxFunc**

  - 描述：用于标记是否保存 endLocation 位置的一条或多条数据，true：不保存，false(默认)：保存， 某些情况下可能出现最后几条数据被重复记录的情况，可以将此参数配置为 true
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

  - 描述：sybase-x
  - 必选：是
  - 参数类型：String
  - 默认值：无
    <br />

- **url**

  - 描述：jdbc:jtds:sybase://hostname:port/database
  - 必选：是
  - 参数类型：String
  - 默认值：无
    <br />

- **schema**

  - 描述：数据库 schema 名
  - 必选：否
  - 参数类型：string
  - 默认值：无
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

  - 描述：每次从数据库中 fetch 大小，单位：条。
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

  - 描述：多并行度读取的切分字段
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

- **scan.order-by.column**
  - 描述：排序字段，用于拼接sql语句中的order by语句
  - 必选：否
  - 参数类型：String
  - 注意：在增量模式中不生效，增量模式始终使用increColumn做order by
  - 默认值：无
    <br />

- **scan.start-location**

  - 描述：增量字段开始位置,如果不指定则先同步所有，然后在增量
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />

- **scan.restore.columnname**

  - 描述：开启了 cp，任务从 sp/cp 续跑字段名称。如果续跑，则会覆盖 scan.start-location 开始位置，从续跑点开始
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />

- **scan.restore.columntype**
  - 描述：开启了 cp，任务从 sp/cp 续跑字段类型
  - 必选：否
  - 参数类型：String
  - 默认值：无
    <br />

## 五、数据类型

| 是否支持 |                           类型名称                           |
| :------: | :----------------------------------------------------------: |
|   支持   | INT、BIT、TINYINT、SMALLINT、UNSIGNED SMALLINT、INT、UNSIGNED INT、BIGINT、UNSIGNED BIGINT、DECIMAL、NUMERIC、FLOAT、DOUBLE、REAL、SMALLMONEY、MONEY、DATE、TIME、BIGTIME、SMALLDATETIME、DATETIME、BIGDATETIME、CHAR、UNICHAR、VARCHAR、UNIVARCHAR、TEXT、UNITEXT、BINARY、VARBINARY、IMAGE、NUMERIC IDENTITY |
|  不支持  |                 ARRAY、MAP、STRUCT、UNION 等                 |

## 六、脚本示例

见项目内`chunjun-examples`文件夹。
