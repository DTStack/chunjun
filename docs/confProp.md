## confProp

 * **early.trigger：开启window统计提前触发功能，单位为秒(填写正整数即可)，如：5。**
 * sql.ttl.min: 最小过期时间,大于0的整数,如1d、1h(d\D:天,h\H:小时,m\M:分钟,s\s:秒)
 * sql.ttl.max: 最大过期时间,大于0的整数,如2d、2h(d\D:天,h\H:小时,m\M:分钟,s\s:秒),需同时设置最小时间,且比最小时间大5分钟
 * state.backend: 任务状态后端，可选为MEMORY,FILESYSTEM,ROCKSDB，默认为flinkconf中的配置。
 * state.checkpoints.dir: FILESYSTEM,ROCKSDB状态后端文件系统存储路径，例如：hdfs://ns1/dtInsight/flink180/checkpoints。
 * state.backend.incremental: ROCKSDB状态后端是否开启增量checkpoint,默认为true。
 * **sql.checkpoint.unalignedCheckpoints：是否开启Unaligned Checkpoint,不开启false,开启true。默认为true。**
 * sql.env.parallelism: 默认并行度设置
 * sql.max.env.parallelism: 最大并行度设置
 * time.characteristic: 可选值[ProcessingTime|IngestionTime|EventTime]
 * sql.checkpoint.interval: 设置了该参数表明开启checkpoint(ms)
 * sql.checkpoint.mode: 可选值[EXACTLY_ONCE|AT_LEAST_ONCE]
 * sql.checkpoint.timeout: 生成checkpoint的超时时间(ms)
 * sql.max.concurrent.checkpoints: 最大并发生成checkpoint数
 * sql.checkpoint.cleanup.mode: 默认是不会将checkpoint存储到外部存储,[true(任务cancel之后会删除外部存储)|false(外部存储需要手动删除)]
 * flinkCheckpointDataURI: 设置checkpoint的外部存储路径,根据实际的需求设定文件路径,hdfs://, file://
 * jobmanager.memory.mb: per_job模式下指定jobmanager的内存大小(单位MB, 默认值:768)
 * taskmanager.memory.mb: per_job模式下指定taskmanager的内存大小(单位MB, 默认值:768)
 * taskmanager.numberOfTaskSlots：per_job模式下指定每个taskmanager对应的slot数量(默认1),通过该参数和sql.env.parallelism可控制tm的个数,即sql.env.parallelism/taskmanager.numberOfTaskSlots 向上取整。
 * savePointPath：任务恢复点的路径（默认无）
 * allowNonRestoredState：指示保存点是否允许非还原状态的标志（默认false）
 * logLevel: 日志级别动态配置（默认info）
 * sample.interval.count：间隔一定数据条数后，将本次进入Flink的数据抽样打印到日志中。默认为0，不进行抽样打印。可以设置一个整数，例如：1000000。
 * [prometheus 相关参数](./prometheus.md) per_job可指定metric写入到外部监控组件,以prometheus pushgateway举例
