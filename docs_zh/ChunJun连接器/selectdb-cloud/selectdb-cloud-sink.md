# Select DB Cloud Sink

## 一、介绍

Select DB Cloud Sink 支持向 SelectDB Cloud 云原生数据库中写入数据

## 二、支持版本

Select DB Cloud 全部版本

## 三、插件名称

| 插件类型 | 插件名称                |
|------|---------------------|
| Sync | selectdbcloudwriter |
| SQL  | selectdbcloud-x     |

## 四、插件参数

### Sync 

- **host**
  - 描述：SelectDB Cloud 仓库地址
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **httpPort**
  - 描述：SelectDB Cloud 仓库 HTTP 端口
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **queryPort**
  - 描述：SelectDB Cloud 仓库查询端口
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **cluster**
  - 描述：SelectDB Cloud 集群名称
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **username**
  - 描述：SelectDB Cloud 仓库用户名
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **password**
  - 描述：SelectDB Cloud 仓库密码
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **table.Identifier**
  - 描述：写入表名称，格式为：数据库名称.数据表名称
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **column**
  - 描述：需要写入的字段，需要说明字段名称和类型，例如："column":[{"name":"id","type":"int"}]
  - 必选：是
  - 字段类型：List
  - 默认值：无

- **batchSize**
  - 描述：数据写入的批次大小
  - 必选：否
  - 字段类型：int
  - 默认值：10000

- **flushIntervalMills**
  - 描述：批量写入时间间隔：单位毫秒
  - 必选：否
  - 字段类型：int
  - 默认值：10000

- **maxRetries**
  - 描述：重试次数
  - 必选：否
  - 字段类型：int
  - 默认值：3

- **enableDelete**
  - 描述：是否开启删除
  - 必选：否
  - 字段类型：bool
  - 默认值：true
  
- **loadProperties**
  - 描述：SelectDB Cloud 写入任务的系统参数
  - 必选：否
  - 字段类型：Object
  - 默认值：无

### SQL

- **host**
  - 描述：SelectDB Cloud 仓库地址
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **http-port**
  - 描述：SelectDB Cloud 仓库 HTTP 端口
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **query-port**
  - 描述：SelectDB Cloud 仓库查询端口
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **cluster-name**
  - 描述：SelectDB Cloud 集群名称
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **username**
  - 描述：SelectDB Cloud 仓库用户名
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **password**
  - 描述：SelectDB Cloud 仓库密码
  - 必选：是
  - 字段类型：String
  - 默认值：无
- **table.Identifier**
  - 描述：写入表名称，格式为：数据库名称.数据表名称
  - 必选：是
  - 字段类型：String
  - 默认值：无

- **sink.batch-size**
  - 描述：数据写入的批次大小
  - 必选：否
  - 字段类型：int
  - 默认值：10000

- **sink.batch.interval**
  - 描述：批量写入时间间隔：单位毫秒
  - 必选：否
  - 字段类型：int
  - 默认值：10000

- **sink.max-retries**
  - 描述：重试次数
  - 必选：否
  - 字段类型：int
  - 默认值：3

- **sink.enable-delete**
  - 描述：是否开启删除
  - 必选：否
  - 字段类型：bool
  - 默认值：true

- **sink.properties.***
  - 描述：SelectDB Cloud 写入任务的系统参数, 例如想要配置 file.type 属性为 json，则配置 'sink.properties.file.type' = 'json'
  - 必选：否
  - 字段类型：String
  - 默认值：无

## 五、数据类型

## 六、脚本示例

见项目内`**chunjun-examples**`文件夹。