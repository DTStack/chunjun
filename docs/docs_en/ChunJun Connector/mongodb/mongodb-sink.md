---
sidebar_position: 6
title: MongoDB Sink
---

## 一、介绍
向MongoDB中写入数据

## 二、支持版本
MongoDB 3.4及以上


## 三、插件名称
| Sync | mongodbsink、mongodbwriter |
| --- | --- |
| SQL | mongodb-x |



## 四、参数说明
### 1、数据同步

- **url**
    - 描述：MongoDB数据库连接的URL字符串，详细请参考[MongoDB官方文档](https://docs.mongodb.com/manual/reference/connection-string/)
    - 必选：否
    - 字段类型：String
    - 默认值：无



- **hostPorts**
    - 描述：MongoDB的地址和端口，格式为 IP1:port，可填写多个地址，以英文逗号分隔
    - 必选：是
    - 字段类型：String
    - 默认值：无



- **username**
    - 描述：数据源的用户名
    - 必选：否
    - 字段类型：String
    - 默认值：无



- **password**
    - 描述：数据源指定用户名的密码
    - 必选：否
    - 字段类型：String
    - 默认值：无



- **database**
    - 描述：数据库名称
    - 必选：否
    - 字段类型：String
    - 默认值：无



- **collectionName**
    - 描述：集合名称
    - 必选：是
    - 字段类型：String
    - 默认值：无



- **replaceKey**
    - 描述：replaceKey 指定了每行记录的业务主键，用来做覆盖时使用（不支持 replaceKey为多个键，一般是指Monogo中的主键）
    - 必选：否
    - 字段类型：String
    - 默认值：无



- **writeMode**
    - 描述：写入模式，当 batchSize > 1 时不支持 replace 和 update 模式
    - 必选：是
    - 所有选项：insert/replace/update
    - 字段类型：String
    - 默认值：insert



- **batchSize**
    - 描述：一次性批量提交的记录数大小，该值可以极大减少ChunJun与MongoDB的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成ChunJun运行进程OOM情况
    - 必选：否
    - 字段类型：int
    - 默认值：1
- **flushIntervalMills**
    - 描述：批量写入时间间隔：单位毫秒。
    - 必选：否
    - 字段类型：int
    - 默认值：10000
### 2、SQL计算
SQL计算暂时只支持INSERT模式，后续可加入如果配置主键则使用UPSERT模式。

- **url**
    - 描述：MongoDB数据库连接的URL字符串，详细请参考[MongoDB官方文档](https://docs.mongodb.com/manual/reference/connection-string/)
    - 必选：是
    - 默认值：无
- **database**
    - 描述：数据库名称
    - 必选：是
    - 默认值：无
- **collection**
    - 描述：集合名称
    - 必选：是
    - 默认值：无
- **username**
    - 描述：数据源的用户名
    - 必选：否
    - 默认值：无
- **password**
    - 描述：数据源指定用户名的密码
    - 必选：否
    - 默认值：无



- **sink.parallelism**
    - 描述：sink并行度
    - 必选：是
    - 默认值：无
- **sink.buffer-flush.max-rows**
    - 描述：批量写入条数
    - 必选：否
    - 默认值：无
- **sink.buffer-flush.interval**
    - 描述：批量写入时间间隔：单位毫秒。
    - 必选：否
    - 默认值：无
## 五、数据类型
| 是否支持 | 类型名称 |
| --- | --- |
| 支持 | long  double  decimal objectId string bindata date timestamp bool |
| 不支持 | array |


## 六、脚本示例
见项目内`chunjun-examples`文件夹。
