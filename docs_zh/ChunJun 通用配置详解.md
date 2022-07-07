# ChunJun 通用配置详解

## 配置文件结构详解

一个完整的 ChunJun 任务脚本配置包含 content， setting 两个部分。content 用于配置任务的输入源与输出源，其中包含 reader，writer。而 setting 则配置任务整体的环境设定，其中包含 speed，errorLimit，metricPluginConf，restore，log，dirty。总体结构如下所示：

```JSON
{
  "job": {
    "content": [
      {
        "reader": {},
        "writer": {}
      }
    ],
    "setting": {
      "speed": {},
      "errorLimit": {},
      "metricPluginConf": {},
      "restore": {},
      "log": {},
      "dirty": {}
    }
  }
}
```

<table>
	<tr>
		<td colspan="2">名称</td>
		<td>说明</td>
		<td>是否必填</td>
	</tr>
	<tr>
		<td rowspan="2">contend</td>
		<td>reader</td>
		<td>reader插件详细配置</td>
		<td>是</td>
	</tr>
  <tr>
		<td>writer</td>
		<td>writer插件详细配置</td>
		<td>是</td>
	</tr>
  <tr>
		<td rowspan="7">setting</td>
		<td>speed</td>
		<td>速率限制</td>
		<td>否</td>
	</tr>
  <tr>
    <td>errorLimit</td>
		<td>出错控制</td>
		<td>否</td>
	</tr>
  <tr>
    <td>metricPluginConf</td>
		<td>指标插件配置</td>
		<td>否</td>
	</tr>
  <tr>
    <td>restore</td>
		<td>任务类型及断点续传配置</td>
		<td>否</td>
	</tr>
  <tr>
    <td>log</td>
		<td>日志记录配置</td>
		<td>否</td>
	</tr>
  <tr>
    <td>dirty</td>
		<td>脏数据保存</td>
		<td>否</td>
	</tr>
</table>

## Content 配置

### Reader

reader 用于配置数据的输入源，即数据从何而来。具体配置如下所示：

```json
"reader" : {
  "name" : "xxreader",
  "parameter" : {
		......
  }
}
```

| 名称      | 说明                                          | 是否必填 |
| --------- | --------------------------------------------- | -------- |
| name      | reader 插件名称，具体名称参考各数据源配置文档 | 是       |
| parameter | 数据源配置参数，具体配置参考各数据源配置文档  | 是       |

### Writer

```JSON
"writer" : {
  "name" : "xxwriter",
  "parameter" : {
		......
  }
}
```

| 名称      | 说明                                          | 是否必填 |
| --------- | --------------------------------------------- | -------- |
| name      | writer 插件名称，具体名称参考各数据源配置文档 | 是       |
| parameter | 数据源配置参数，具体配置参考各数据源配置文档  | 是       |

## Setting 配置

### speed

speed 用于配置任务并发数及速率限制。具体配置如下所示：

```json
"speed" : {
  "channel": 1,
  "readerChannel": -1,
  "writerChannel": -1,
  "bytes": 0,
  "rebalance" : true
}
```

| 名称          | 说明                                   | 是否必填 | 默认值 | 数据类型 |
| ------------- | -------------------------------------- | -------- | ------ | -------- |
| channel       | 任务并发数                             | 否       | 1      | Integer  |
| readerChannel | source 并行度，-1 代表采用全局并行度   | 否       | -1     | Integer  |
| writerChannel | sink 并行度，-1 代表采用全局并行度     | 否       | -1     | Integer  |
| bytes         | bytes >0 则表示开启任务限速            | 否       | 0      | Long     |
| rebalance     | 是否强制进行 rebalance，开启会消耗性能 | 否       | false  | Boolean  |

### ErrorLimit

errorLimit 用于配置任务运行时数据读取写入的出错控制。具体配置如下所示：

```json
"errorLimit" : {
  "record": 100,
  "percentage": 10.0
}
```

| 名称       | 说明                                             | 是否必填 | 默认值 | 参数类型 |
| ---------- | ------------------------------------------------ | -------- | ------ | -------- |
| record     | 错误阈值，当错误记录数超过此阈值时任务失败       | 否       | 0      | Integer  |
| percentage | 错误比例阈值，当错误记录比例超过此阈值时任务失败 | 否       | 0.0    | Double   |

### MetricPluginConf

metricPluginConf 用于配置 chunjun 指标相关信息。

目前只应用于 Jdbc 插件中，在作业结束时将 StartLocation 和 EndLocation 指标发送到指定数据源中。

目前支持 Prometheus 和 Mysql。具体配置如下所示：

#### Prometheus

promethusReporter 依赖 pushGateway 和普罗米修斯交互

```json
"metricPluginConf" : {
  "pluginName": "promethus"
}
```

普罗米修斯的相关配置信息需要配置在 flink 的配置文件中

```yaml
metrics.reporter.promgateway.host: 127.0.0.1
metrics.reporter.promgateway.port: 9091
metrics.reporter.promgateway.jobName: testjob
metrics.reporter.promgateway.randomJobNameSuffix: true
metrics.reporter.promgateway.deleteOnShutdown: false
```

| 名称                                             | 说明                                     | 是否必填 | 默认值 |
| ------------------------------------------------ | ---------------------------------------- | -------- | ------ |
| metrics.reporter.promgateway.host                | pushGateway 域名                         | 是       | 无     |
| metrics.reporter.promgateway.port                | pushGateway 端口                         | 是       | 0      |
| metrics.reporter.promgateway.jobName             | 指标对应作业名                           | 否       | 无     |
| metrics.reporter.promgateway.randomJobNameSuffix | 是否对作业名加上随机后缀，防止作业名重复 | 否       | false  |
| metrics.reporter.promgateway.deleteOnShutdown    | 是否在作业结束后删除指标信息             | 否       | true   |

#### Mysql

要求目标表存在至少两个 String 字段，metric_name 和 metric_value，分别记录指标名和指标值

```json
"metricPluginConf" : {
  "pluginName": "promethus"
  "pluginProp": {
    "jdbcUrl":"",
  	"database":"",
  	"table":"",
  	"username":"",
  	"password":"",
  	"properties":{
    }
  }
}
```

| 名称       | 说明                          | 是否必填 | 默认值 | 参数类型 |
| ---------- | ----------------------------- | -------- | ------ | -------- |
| jdbcUrl    | 针对 Mysql 的 jdbc 连接字符串 | 是       | 无     | String   |
| table      | mysql 表名                    | 是       | 无     | String   |
| username   | mysql 用户名                  | 是       | 无     | String   |
| password   | mysql 用户名对应的密码        | 是       | 无     | String   |
| properties | 数据库连接配置                | 否       | 无     | Map      |

### Restore

restore 用于配置同步任务类型（离线同步、实时采集）和断点续传功能。具体配置如下所示：

```json
"restore" : {
  "isStream" : false,
  "isRestore" : false,
  "restoreColumnName" : "",
  "restoreColumnIndex" : 0
}
```

| 名称               | 说明                | 是否必填           | 默认值 | 参数类型 |
| ------------------ | ------------------- | ------------------ | ------ | -------- |
| isStream           | 是否为实时采集任务  | 否                 | false  | Boolean  |
| isRestore          | 是否开启断点续传    | 否                 | false  | Boolean  |
| restoreColumnName  | 断点续传字段名称    | 开启断点续传后必填 | 无     | String   |
| restoreColumnIndex | 断点续传字段索引 ID | 开启断点续传后必填 | 无     | Integer  |

### Log

log 用于配置 ChunJun 中定义的插件日志的保存与记录。具体配置如下所示：

```json
"log" : {
  "isLogger": false,
  "level" : "info",
  "path" : "/tmp/dtstack/chunjun/",
  "pattern":""
}
```

| 名称     | 说明                 | 是否必填 | 默认值                                                                                                                                                      | 参数类型 |
| -------- | -------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| isLogger | 是否保存日志记录     | 否       | false                                                                                                                                                       | Boolean  |
| level    | 日志级别             | 否       | info                                                                                                                                                        | String   |
| path     | 服务器上日志保存路径 | 否       | /tmp/dtstack/chunjun/                                                                                                                                        | String   |
| pattern  | 日志输出格式         | 否       | log4j：%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{60} %X{sourceThread} - %msg%n<br />logback : %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n | String   |
