ChunJun使用了flink内置Accumulator和Metric来记录任务的一些统计指标：

| 指标名称             | 含义          |
| ---------------- | ----------- |
| numRead          | 累计读取数据条数    |
| byteRead         | 累计读取数据字节数   |
| readDuration     | 读取数据的总时间    |
|                  |             |
| numWrite         | 累计写入数据条数    |
| byteWrite        | 累计写入数据字节数   |
| writeDuration    | 写入数据的总时间    |
| nErrors          | 累计错误记录数     |
| nullErrors       | 累计空指针错误记录数  |
| duplicateErrors  | 累计主键冲突错误记录数 |
| conversionErrors | 累计类型转换错误记录数 |
| otherErrors      | 累计其它错误记录数   |

### 获取统计指标的方式

#### 1.Local模式运行

local模式运行时，任务结束后会在控制台打印这些指标：

```
---------------------------------
numWrite                  |  100
last_write_num_0          |  0
conversionErrors          |  0
writeDuration             |  12251
numRead                   |  100
duplicateErrors           |  0
snapshotWrite             |  0
readDuration              |  12247
otherErrors               |  0
byteRead                  |  2329
last_write_location_0     |  0
byteWrite                 |  2329
nullErrors                |  0
nErrors                   |  0
---------------------------------
```

#### 2.yarn模式运行

##### 2.1 通过Flink REST接口获取

任务运行期间，可以通过Flink REST接口获取Accumulator数据，名称和上面给出的一致。

api：http://host:8088/proxy/application_1569335225689_4172//jobs/d5582272d29ff38e10416a4043a86cad/accumulators

返回数据示例：

```json
{
    "job-accumulators": [],
    "user-task-accumulators": [
        {
            "name": "numWrite",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "last_write_num_0",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "conversionErrors",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "writeDuration",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "numRead",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "duplicateErrors",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "snapshotWrite",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "readDuration",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "otherErrors",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "byteRead",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "last_write_location_0",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "byteWrite",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "nullErrors",
            "type": "LongCounter",
            "value": "0"
        },
        {
            "name": "nErrors",
            "type": "LongCounter",
            "value": "0"
        }
    ],
    "serialized-user-task-accumulators": {}
}
```

##### 2.2 将指标输出到其它系统

比如将指标输出到prometheus，在flink的配置文件里增加配置即可：

```
metrics.reporter.promgateway.class: org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter
metrics.reporter.promgateway.interval: 500 MILLISECONDS 
metrics.reporter.promgateway.host: 127.0.0.1 
metrics.reporter.promgateway.port: 9091
metrics.reporter.promgateway.jobName: testjob
metrics.reporter.promgateway.randomJobNameSuffix: true
metrics.reporter.promgateway.deleteOnShutdown: false
```

通过prometheus获取数据时的名称为：

| ChunJun中指标名称      | prometheus中指标名称                                             |
| ---------------- | ----------------------------------------------------------- |
| numRead          | flink_taskmanager_job_task_operator_chunjun_byteRead         |
| byteRead         | flink_taskmanager_job_task_operator_chunjun_byteRead         |
| readDuration     | flink_taskmanager_job_task_operator_chunjun_readDuration     |
|                  |                                                             |
| numWrite         | flink_taskmanager_job_task_operator_chunjun_numWrite         |
| byteWrite        | flink_taskmanager_job_task_operator_chunjun_byteWrite        |
| writeDuration    | flink_taskmanager_job_task_operator_chunjun_writeDuration    |
| nErrors          | flink_taskmanager_job_task_operator_chunjun_nErrors          |
| nullErrors       | flink_taskmanager_job_task_operator_chunjun_nullErrors       |
| duplicateErrors  | flink_taskmanager_job_task_operator_chunjun_duplicateErrors  |
| conversionErrors | flink_taskmanager_job_task_operator_chunjun_conversionErrors |
| otherErrors      | flink_taskmanager_job_task_operator_chunjun_otherErrors      |
