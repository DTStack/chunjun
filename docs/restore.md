# 断点续传

## 1.什么是断点续传

**断点续传**是指同步任务在运行过程中因某种原因导致任务失败，无需重跑整个任 务，只需从上次失败的位置继续同步即可，类似于下载文件时因网络原因失败，不 需要重新下载文件，只需要继续下载就行。

同步任务支持断点续传的好处：

- 节省时间：不需要重跑任务，只需要从断点继续同步，节省重跑的时间；

- 节省资源：如果数据量巨大，重跑整个任务会占用集群资源，影响其它任务运 行； 

- 减少运维成本：断点续传结合任务的失败重试机制，可以让任务自己运行完成， 不人为干涉；

## 2.工作原理

##### 2.1 checkpoint

断点续传基于flink的checkpoint功能实现，Checkpoint是Flink实现容错机制最核心的功能，它能够根据配置周期性地对任务中的Operator/task的状态生成快照，从而将这些状态数据定期持久化存储下来，当Flink程序一旦意外崩溃时，重新运行程序时可以有选择地从这些快照进行恢复，从而修正因为故障带来的程序数据异常。支持快照恢复的数据源必须支持在一定时间内重放事件，比如Kafka。

##### 2.2 从断点读取

当前支持的数据源中可以实现数据重放的有关系数据库，ES，MongoDb等，只要是能根据过滤条件查询的数据源都满足条件。对于关系数据库，任务的状态就是恢复字段的读取位置，任务恢复就是拿到待恢复的快照信息，将里面的位置信息拼接到查询sql的过滤条件里，然后进行数据查询。目前只实现了关系数据库的失败恢复。

```
select
id, name, address, email
from
student
where
id > ${restoreLocatio}
```

##### 2.3 写入控制

**写入hdfs和ftp文件**时，是先将数据写入临时文件，flink出发checkpoint生成时将临时文件转为正式的数据文件；

**写入关系数据库**是通过事务控制的，关闭事务自动提交功能，flink出发checkpoint生成时提交事务；

##### 2.4 要求

数据要求：断点续传要求数据按照某个字段升序排列，比如id字段或时间字段。

数据库：要求必须支持事务。

## 3.如何配置

3.1 启动参数

```
-confProp 
"{"flink.checkpoint.interval":60000,"flink.checkpoint.stateBackend":"/flink_checkpoint/"}"

-s 
/flink_checkpoint/0481473685a8e7d22e7bd079d6e5c08c/chk-*
```

- confProp：checkpoint的配置参数
  
  - flink.checkpoint.interval：checkpoint时间间隔，不填此参数，checkpoint不会弃用；
  
  - flink.checkpoint.timeout：超时时间；
  
  - flink.checkpoint.stateBackend：checkpoint的路径，目前只支持本地文件系统和hdfs路径；

- s：恢复任务时checkpoint的路径

3.2 任务配置

```
"restore": {
     "isRestore": false,
     "restoreColumnName": "",
     "restoreColumnIndex": 0
 }
```

- isRestore：断点续传开关，默认为false

- restoreColumnName：用来做断点续传的字段名称

- restoreColumnIndex：字段索引

## 4 支持的插件

| reader     | writer                                |
| ---------- | ------------------------------------- |
| mysql      | hdfs                                  |
| oracle     | ftp                                   |
| sqlserver  | mysql，oracle，sqlserver，db2，postgresql |
| db2        |                                       |
| postgresql |                                       |
