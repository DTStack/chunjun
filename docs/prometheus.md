## 使用 prometheus pushgateway 需要设置的 confProp 参数

* metrics.reporter.promgateway.class: org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter
* metrics.reporter.promgateway.host: prometheus pushgateway的地址
* metrics.reporter.promgateway.port：prometheus pushgateway的端口
* metrics.reporter.promgateway.jobName: 实例名称
* metrics.reporter.promgateway.randomJobNameSuffix: 是否在实例名称后面添加随机字符串(默认:true)
* metrics.reporter.promgateway.deleteOnShutdown: 是否在停止的时候删除数据(默认false)
