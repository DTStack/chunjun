# Doris batch Sink

## 一、介绍
Doris batch Sink插件支持向Doris数据库写入数据

## 二、支持版本
Doris  0.14.x

## 三、插件名称
| Sync | dorisbatchsink、dorisbatchwriter |
| --- | --- |
| SQL |  |

## 四、插件参数


### 1.Sync

- **feNodes**
  - 描述：连接Doris的Fe Nodes 地址
  - 必选：是
  - 字段类型：List
    - 示例：
```json
"feNodes": ["127.0.0.1:8030"]
```

- 默认值：无

<br />


- **table**
  - 描述：目的表的表名称。目前只支持配置单个表，后续会支持多表
  - 必选：是
  - 字段类型：List
  - 默认值：无


<br />

- **database**
  - 描述：写入Doris的库名
  - 必选：是
  - 字段类型：String
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
  - 字段类型：List
  - 默认值：无


<br />

- **fieldDelimiter**
  - 描述：写入Doris数值的字段分隔符
  - 必选：否
  - 字段类型：String
  - 默认值：\t


<br />

- **lineDelimiter**
  - 描述：写入Doris数值的行分隔符
  - 必选：否
  - 字段类型：String
  - 默认值：\n


<br />

- **loadProperties**
  - 描述：针对Doris写入任务的系统参数，主要是针对Doris的特定配置
  - 必选：否
  - 字段类型：Object
  - 默认值：无


<br />

- **batchSize**
  - 描述：批量写入Doris的数据量大小
  - 必选：否
  - 字段类型：int
  - 默认值：1000


<br />






## 五、数据类型

| 是否支持 | 数据类型                                                     |
| -------- | ------------------------------------------------------------ |
| 支持     | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL、DATE、DATETIME、CHAR、VARCHAR、STRING |


## 六、脚本示例
见项目内`**chunjun-examples**`文件夹。
