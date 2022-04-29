## confProp

* table.exec.source.idle-timeout：当一个源在超时时间内没有收到任何元素时，它将被标记为临时空闲。这允许下游任务推进其水印，而无需在空闲时等待来自该源的水印。默认值为 0（表示未启用检测源空闲），可设置：10 ms(单位毫秒)。


* table.exec.emit.early-fire.enabled：开启window统计提前触发功能。默认:false（表示不开启），设置true开启。
* table.exec.emit.early-fire.delay：开启window统计提前触发时间,上面设置为true才有效。无默认值，可设置：1s（单位为秒）。


* table.exec.state.ttl：状态最小过期时间。默认:0 ms（代表不过期）。


* table.exec.mini-batch.enabled：是否开启minibatch，可以减少状态开销。这可能会增加一些延迟，因为它会缓冲一些记录而不是立即处理它们。这是吞吐量和延迟之间的权衡。默认:false（表示不开启），设置true开启
* table.exec.mini-batch.allow-latency：状态缓存时间，table.exec.mini-batch.enabled为true才有效。无默认，可设置：5 s（单位为秒）。
* table.exec.mini-batch.size：状态最大缓存条数，table.exec.mini-batch.enabled为true才有效。无默认，可设置：5000（单位为条数）。
* table.optimizer.agg-phase-strategy：是否开启Local-Global聚合，前提需要开启minibatch，聚合是为解决数据倾斜问题提出的，类似于 MapReduce 中的 Combine + Reduce 模式。无默认，可设置：TWO_PHASE。


* table.optimizer.distinct-agg.split.enabled：是否开启拆分distinct聚合，Local-Global可以解决数据倾斜，但是在处理distinct聚合时，其性能并不令人满意，如：SELECT day, COUNT(DISTINCT user_id) FROM T GROUP BY day 如果 distinct key （即 user_id）的值分布稀疏，建议开启。无默认，可设置：true。
* 其他一些sql相关配置参考：https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/config.html


* sql.checkpoint.interval: 设置了该参数表明开启checkpoint(ms)
* sql.checkpoint.unalignedCheckpoints：是否开启Unaligned Checkpoint,不开启false,开启true。默认为：false。
* sql.checkpoint.mode: 可选值[EXACTLY_ONCE|AT_LEAST_ONCE]
* sql.checkpoint.timeout: 生成checkpoint的超时时间(ms)
* sql.max.concurrent.checkpoints: 最大并发生成checkpoint数
* sql.checkpoint.cleanup.mode: 默认是不会将checkpoint存储到外部存储,[true(任务cancel之后会删除外部存储)|false(外部存储需要手动删除)]


* state.backend: 任务状态后端，可选为MEMORY,FILESYSTEM,ROCKSDB，默认为flinkconf中的配置。
* state.checkpoints.dir: FILESYSTEM,ROCKSDB状态后端文件系统存储路径，例如：hdfs://ns1/dtInsight/flink180/checkpoints。
* state.backend.incremental: ROCKSDB状态后端是否开启增量checkpoint,默认为true。
* 其他一些state相关配置参考：https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/state/checkpointing.html


* sql.env.parallelism: 默认并行度设置
* sql.max.env.parallelism: 最大并行度设置


* time.characteristic: 可选值[ProcessingTime|IngestionTime|EventTime]


* jobmanager.memory.process.size: per_job模式下指定jobmanager的内存大小(单位MB, 默认值:1600m)
* taskmanager.memory.process.size: per_job模式下指定taskmanager的内存大小(单位MB, 默认值:1728m)
* taskmanager.numberOfTaskSlots：per_job模式下指定每个taskmanager对应的slot数量(默认1),通过该参数和sql.env.parallelism可控制tm的个数,即sql.env.parallelism/taskmanager.numberOfTaskSlots 向上取整。
* s：任务恢复点的路径（默认无）
* allowNonRestoredState：指示保存点是否允许非还原状态的标志（默认false）
* logLevel: 日志级别动态配置（默认info）
* [prometheus 相关参数](./prometheus.md) per_job可指定metric写入到外部监控组件,以prometheus pushgateway举例
