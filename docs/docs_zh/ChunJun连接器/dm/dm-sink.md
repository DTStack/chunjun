# DM Sink

## 一、介绍

DM sink

## 二、支持版本

DM7、DM8


## 三、插件名称

| Sync | dmsink、dmwriter |
| ---- |-----------------|
| SQL  |                 |


## 四、参数说明

### 1、Sync

- **connection**

    - 描述：数据库连接参数，包含jdbcUrl、schema、table等参数

    - 必选：是

    - 参数类型：List

    - 默认值：无

      ```text
      "connection": [{
       "jdbcUrl": ["jdbc:dm://localhost:5236"],
       "table": ["table"],
       "schema":"public"
      }]
      ```

       <br />

- **jdbcUrl**

    - 描述：针对关系型数据库的jdbc连接字符串,[达梦官方文档](http://www.dameng.com/down.aspx?TypeId=12&FId=t14:12:14)
    - 必选：是
    - 参数类型：string
    - 默认值：无
      <br />

- **schema**

    - 描述：数据库schema名
    - 必选：否
    - 参数类型：string
    - 默认值：用户名
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

- **column**

    - 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]
    - 必选：是
    - 参数类型：List
    - 默认值：无
      <br />

- **fullcolumn**

    - 描述：目的表中的所有字段，字段之间用英文逗号分隔。例如: "column": ["id","name","age","hobby"]，如果不配置，将在系统表中获取
    - 必选：否
    - 参数类型：List
    - 默认值：无
      <br />

- **preSql**

    - 描述：写入数据到目的表前，会先执行这里的一组标准语句
    - 必选：否
    - 参数类型：List
    - 默认值：无
      <br />

- **postSql**

    - 描述：写入数据到目的表后，会执行这里的一组标准语句
    - 必选：否
    - 参数类型：List
    - 默认值：无
      <br />

- **writeMode**

    - 描述：控制写入数据到目标表采用 insert into 或者 merge into 语句
    - 必选：是
    - 所有选项：insert/update
    - 参数类型：String
    - 默认值：insert
      <br />

- **batchSize**

    - 描述：一次性批量提交的记录数大小，该值可以极大减少ChunJun与数据库的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成ChunJun运行进程OOM情况
    - 必选：否
    - 参数类型：int
    - 默认值：1024
      <br />

- **updateKey**

    - 描述：当写入模式为update时，需要指定此参数的值为唯一索引字段
    - 注意：
        - 如果此参数为空，并且写入模式为update时，应用会自动获取数据库中的唯一索引；
        - 如果数据表没有唯一索引，但是写入模式配置为update和，应用会以insert的方式写入数据；
    - 必选：否
    - 参数类型：Map<String,List>
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

- **compatibleMode**

    - 描述：sink端是否支持二阶段提交
    - 注意：
        - 如果此参数为空，默认不开启二阶段提交，即sink端不支持exactly_once语义；
        - 当前只支持exactly-once 和at-least-once
    - 必选：否
    - 参数类型：String
        - 示例："semantic": "exactly-once"
    - 默认值：at-least-once
      <br />


## 五、数据类型


| 是否支持 | 数据类型                                                     |
| -------- | ------------------------------------------------------------ |
| 支持     | CHAR、CHARACTER、VARCHAR、VARCHAR2、CLOB、TEXT、LONG、LONGVARCHAR、ENUM、SET、JSON、DECIMAL、NUMBERIC、DEC、NUMER、INT、INTEGER、TINYINT、BYTE、BYTES、SMALLINT、BIGINT、BINARY、VARBINARY、BLOB、TINYBLOB、MEDIUMBLOB、LONGBLOB、GEOMETER、IMAGE、REAL、FLOAT、DOUBLE、DOUBLE PRECISION、BIT、YEAR、DATE、TIME、TIMESTAMP、DATETIME |
| 不支持   | PLS_INTEGER、LONGVARBINARY、BFILE、TIME WITH TIME ZONE、TIMESTAMP WITH TIME ZONE、TIME WITH LOCAL TIME ZONE、INTERVAL YEAR、INTERVAL YEAR、INTERVAL MONTH、INTERVAL DAY、INTERVAL HOUR、INTERVAL MINUTE、INTERVAL SECONDE、INTERVAL YEAR TO MONTH、INTERVAL DAY TO HOUR、INTERVAL YEAR TO MINUTE、INTERVAL DAY TO SECONDE、INTERVAL HOUR TO MINUTE、INTERVAL HOUR TO SECOND、INTERVAL MINUTE TO SECONDE、BOOL、BOOLEAN、%TYPE%、%ROWTYPE、记录类型、数组类型、集合类型 |


## 六、脚本示例

见项目内`chunjun-examples`文件夹。

