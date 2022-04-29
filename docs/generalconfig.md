# 插件通用配置

## 一、Sync

### 配置文件

一个完整的Flinkx任务脚本配置包含 content，setting两个部分。content用于配置任务的输入源、输出源以及数据转换规则，其中包含reader，writer，transformer。setting则配置任务整体的环境设定，其中包含speed，errorLimit，dirty。具体如下所示：

```json
{
  "job": {
    "content": [
      {
        "reader": {},
        "writer": {},
        "transformer": {}
      }
    ],
    "setting": {
      "speed": {},
      "errorLimit": {},
      "dirty": {}
    }
  }
}
```

| 名称 |  | 说明 | 是否必填 |
| --- | --- | --- | --- |
| content | reader | reader插件详细配置 | 是 |
|  | writer | writer插件详细配置 | 是 |
|  | transformer | 数据转换SQL | 否 |
| setting | speed | 速率限制 | 否 |
|  | errorLimit | 出错控制 | 否 |
|  | dirty | 脏数据保存 | 否 |

### content配置

#### reader

reader用于配置数据的输入源，即数据从何而来。具体配置如下所示：

```json
{
"reader" : {
  "name" : "xxreader",
  "parameter" : {},
  "table": {
    "tableName": "xxx"
    }
  }
}
```

| 名称 | 说明 | 是否必填 |
| --- | --- | --- |
| name | reader插件名称，具体名称参考各数据源配置文档 | 是 |
| parameter | 数据源配置参数，具体配置参考各数据源配置文档 | 是 |
| table | SQL源表名称 | 开启transformer后必填 |

### writer

writer用于配置数据的输出源，即数据写往何处。具体配置如下所示：

```json
{
"writer" : {
  "name" : "xxwriter",
  "parameter" : {},
  "table": {
    "tableName": "xxx"
    }
  }
}
```

| 名称 | 说明 | 是否必填 |
| --- | --- | --- |
| name | writer插件名称，具体名称参考各数据源配置文档 | 是 |
| parameter | 数据源配置参数，具体配置参考各数据源配置文档 | 是 |
| table | SQL结果表名称 | 开启transformer后必填 |

### transformer配置

transformer用于配置数据转换SQL，支持所有Flink原生语法及Function

```json
{
"transformer" : {
  "transformSql": "xxx"
  }
}
```

### setting配置

#### speed

speed用于配置任务并发数及速率限制。具体配置如下所示：

```json
{
"speed" : {
  "channel": 1,
  "readerChannel": 2,
  "writerChannel": 2,
  "bytes": 0
}
}
```

| 名称 | 说明 | 是否必填 | 默认值 | 参数类型 |
| --- | --- | --- | --- | --- |
| channel | 整体任务并行度 | 否 | 1 | int |
| readerChannel | source并行度 | 否 | -1 | int |
| writerChannel | sink并行度 | 否 | -1 | int |
| bytes | bytes >0则表示开启任务限速 | 否 | 0 | Long |

#### errorLimit

errorLimit用于配置任务运行时数据读取写入的出错控制。具体配置如下所示：

```json
{
"errorLimit" : {
  "record": 100,
  "percentage": 10.0
}
}
```

| 名称 | 说明 | 是否必填 | 默认值 | 参数类型 |
| --- | --- | --- | --- | --- |
| record | 错误阈值，当错误记录数超过此阈值时任务失败 | 否 | 0 | int |
| percentage | 错误比例阈值，当错误记录比例超过此阈值时任务失败 | 否 | 0.0 | Double |

####  

metricPluginConf用于配置任务运行时自定义指标持久化的方式。具体配置如下所示：

```json
{
"metricPluginConf":{
          "pluginName": "mysql",
          "pluginProp": {
            "jdbcUrl": "jdbc:mysql://localhost:3306/ide?useUnicode=true&characterEncoding=utf-8",
            "schema": "ide",
            "table": "flinkx_metrics",
            "username": "drpeco",
            "password": "DT@Stack#123"
          }
}
}
```

| 名称 | 说明 | 是否必填 | 默认值 | 参数类型 |
| --- | --- | --- | --- | --- |
| pluginName | 持久化插件的名称 | 否 | prometheus | String |
| pluginProp | 连接插件需要用到的参数配置 | 否 | 无 | Map |

#### dirty

dirty用于配置脏数据的保存，通常与上文出错控制联合使用。具体配置如下所示：

```json
{
"dirty" : {
  "path" : "xxx",
  "hadoopConfig" : {
  }
 }
}
```

| 名称 | 说明 | 是否必填 | 默认值 | 参数类型 |
| --- | --- | --- | --- | --- |
| path | 脏数据保存路径 | 是 | 无 | Sring |
| hadoopConfig | Hadoop相关配置 | 是 | 无 | K-V键值对 |

参考模板如下：

```json
{
"dirty" : {
        "path" : "/user/hive/warehouse/xx.db/xx",
        "hadoopConfig" : {
          "fs.default.name": "hdfs://0.0.0.0:9000",
          "dfs.ha.namenodes.ns1" : "nn1,nn2",
          "dfs.namenode.rpc-address.ns1.nn1" : "0.0.0.0:9000",
          "dfs.namenode.rpc-address.ns1.nn2" : "0.0.0.1:9000",
          "dfs.client.failover.proxy.provider.ns1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
          "dfs.nameservices" : "ns1"
        }
  }
}
```

## 二、SQL

[参考Flink官方文档](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/)

