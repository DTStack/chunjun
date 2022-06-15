# SqlServer Sink

## 一、介绍
SqlServer Sink插件支持向SqlServer数据库写入数据

## 二、支持版本
Microsoft SQL Server 2012及以上

## 三、插件名称
| Sync | sqlserverwriter、sqlserversink |
| --- | --- |
| SQL | sqlserver-x |

## 四、插件参数

### 1.Sync

- **connection**
   - 描述：数据库连接参数，包含jdbcUrl、schema、table等参数
   - 必选：是
   - 字段类型：List
      - 示例：指定jdbcUrl、schema、table
```json
"connection": [{
     "jdbcUrl": "jdbc:jtds:sqlserver://0.0.0.1:1433;DatabaseName=DTstack",
     "table": ["table"],
  	 "schema":"public"
    }]
```

   - 默认值：无


<br />

- **jdbcUrl**
   - 描述：使用开源的jtds驱动连接 而非Microsoft的官方驱动<br />jdbcUrl参考文档：[jtds驱动官方文档](http://jtds.sourceforge.net/faq.html)
   - 必选：是
   - 字段类型：String
   - 默认值：无


<br />

- **schema**
   - 描述：数据库schema名
   - 必选：否
   - 字段类型：String
   - 默认值：无


<br />

- **table**
   - 描述：目的表的表名称。目前只支持配置单个表，后续会支持多表
   - 必选：是
   - 字段类型：List
   - 默认值：无


<br />

- **username**
   - 描述：数据源的用户名
   - 必选：是
   - 字段类型：String
   - 默认值：无


<br />

- **password**
   - 描述：数据源指定用户名的密码
   - 必选：是
   - 字段类型：String
   - 默认值：无


<br />

- **column**
   - 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]
   - 必选：是
   - 默认值：否
   - 字段类型：List
   - 默认值：无


<br />

- **fullcolumn**
   - 描述：目的表中的所有字段，字段之间用英文逗号分隔。例如: "column": ["id","name","age","hobby"]，如果不配置，将在系统表中获取
   - 必选：否
   - 字段类型：List
   - 默认值：无


<br />

- **presql**
   - 描述：写入数据到目的表前，会先执行这里的一组标准语句
   - 必选：否
   - 字段类型：List
   - 默认值：无

​<br />

- **postSql**
   - 描述：写入数据到目的表后，会执行这里的一组标准语句
   - 必选：否
   - 字段类型：List
   - 默认值：无

<br/>

- **writeMode**
   - 描述：控制写入数据到目标表采用 `insert into` 或者` merge into` 语句
   - 必选：是
   - 所有选项：insert/update
   - 字段类型：String
   - 默认值：insert

<br />

- **withNoLock**
   - 描述：是否在sql语句后面添加 with(nolock)
   - 必选：否
   - 字段类型：Boolean
   - 默认值：false

<br />

- **updateKey**
   - 描述：当写入模式为update时，需要指定此参数的值为唯一索引字段
   - 注意：
      - 采用`merge into`语法，对目标表进行匹配查询，匹配成功时更新，不成功时插入；
   - 必选：否
   - 字段类型：Map<String,List>
      - 示例："updateKey": {"key": ["id"]}
   - 默认值：无


<br />

- **semantic**
  - 描述：sink端是否支持二阶段提交
  - 注意：
    - 如果此参数为空，默认不开启二阶段提交，即sink端不支持exactly_once语义；
    - 当前只支持exactly-once 和at-least-once 
  - 必选：否
  - 参数类型：String
    - 示例："semantic": "exactly-once"
  - 默认值：at-least-once
<br />

- **batchSize**
   - 描述：一次性批量提交的记录数大小，该值可以极大减少ChunJun与数据库的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成ChunJun运行进程OOM情况
   - 必选：否
   - 字段类型：int
   - 默认值：1024



### 2.SQL

- **connector**
   - 描述：connector type
   - 必选：是
   - 字段类型：String
   - 值：sqlserver-x

<br/>

- **url**
   - 描述：使用使用开源的jtds驱动连接 而非Microsoft的官方驱动
   - 必选：是
   - 字段类型：String
   - 默认值：无

<br />

- **schema**
  - 描述：数据库schema名
  - 必选：否
  - 参数类型：string
  - 默认值：无
    <br />

- **table-name**
   - 描述：表名
   - 必选：是
   - 字段类型：String
   - 默认值：无

​<br />

- **schema**
   - 描述：schema
   - 必选：否
   - 字段类型：String
   - 默认值：无

<br/>

- **username**
   - 描述：用户名
   - 必选：是
   - 字段类型：String
   - 默认值：无

​<br />

- **password**
   - 描述：密码
   - 必选：是
   - 字段类型：String
   - 默认值：无

<br/>

- **sink.buffer-flush.max-rows**
   - 描述：批量写数据条数，单位：条
   - 必选：否
   - 参数类型：String
   - 默认值：1024

<br/>

- **sink.buffer-flush.interval**
   - 描述：批量写时间间隔，单位：毫秒
   - 必选：否
   - 参数类型：String
   - 默认值：10000

<br/>

- **sink.all-replace**
   - 描述：是否全部替换数据库中的数据(如果数据库中原值不为null,新值为null,如果为true则会替换为null)
   - 必选：否
   - 参数类型：String
   - 默认值：false

<br/>

- **sink.semantic**
  - 描述：sink端是否支持二阶段提交
  - 注意：
    - 如果此参数为空，默认不开启二阶段提交，即sink端不支持exactly_once语义；
    - 当前只支持exactly-once 和at-least-once 
  - 必选：否
  - 参数类型：String
    - 示例："semantic": "exactly-once"
  - 默认值：at-least-once
<br />

- **sink.parallelism**
   - 描述：写入结果的并行度
   - 必选：否
   - 参数类型：String
   - 默认值：无



## 五、数据类型
|是否支持 | 类型名称 |
| ---| ---|
|支持 | BIT、INT、SMALLINT、TINYINT、BIGINT、INT IDENTITY、REAL、FLOAT、DECIMAL、NUMERIC、CHAR、VARCHAR、VARCHAR(MAX)、TEXT、XML、NCHAR、NVARCHAR、NVARCHAR(MAX)、NTEXT、TIME、DATE、DATETIME、DATETIME2、SMALLDATETIME、DATETIMEOFFSET、TIMESTAMP、BINARY、VARBINARY、IMAGE、MONEY、SMALLMONEY、UNIQUEIDENTIFIER |
| 不支持 | CURSOR、ROWVERSION、HIERARCHYID、SQL_VARIANT、SPATIAL GEOMETRY TYPE、SPATIAL GEOGRAPHY TYPE、TABLE |
## 六、脚本示例
见项目内`chunjun-examples`文件夹。
