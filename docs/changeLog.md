# FlinkX 1.12 新特性

## 1、FlinkX与FlinkStreamSQL融合

FlinkX作为分布式数据同步工具，FlinkStreamSQL基于开源的flink对实时sql进行扩展，我们将二者融合。融合后的FlinkX既支持原有的数据同步、实时采集，也支持SQL流与维表的Join，实现了一套插件完成数据的同步、转换与计算。

## 2、FlinkX增加transformer算子，支持SQL转换

在1.10及之前版本的FlinkX中，我们其实是不支持数据转换的，这对于ETL作业来说几乎是断了一条腿。在1.12版本，我们增加了transformer算子，用户在脚本中定义好数据类型以及SQL转换逻辑，FlinkX将会帮用户把ETL作业一步到位。在配置的SQL中，我们支持所有Flink原生语法及Function。

## 3、FlinkX插件Connector化

在1.10及之前版本的FlinkX中，我们的插件分为reader和writer。在1.12中，我们向Flink社区靠齐，插件不区分为reader和writer，统一命名为connector并遵循社区的规范。统一后的FlinkX connector与社区保持兼容，既社区可以使用FlinkX的connector，FlinkX也可以使用社区的connector。

## 4、FlinkX数据结构优化

在1.10及之前版本的FlinkX中，数据传输使用的是Row，在1.12中，我们向Flink社区靠齐，修改成了RowData。在之前版本实时采集到的数据在Row中是一个Map结构，没有平铺展开导致其实无法写到数据库对应的字段列的。在新版本中，我们将其展开使得实时采集的数据可以写到对应的字段列。这为后续异构数据源的数据还原迈下了坚实的一步。 ​

## 5、FlinkX支持二阶段提交

目前FlinkX几乎所有插件都支持二阶段提交。

## 6、FlinkX支持数据湖 Iceberg

可以流式读取和写入Iceberg数据湖，未来也会加入Hudi支持。

## 7、FlinkX支持提交kubernetes

FlinkX支持使用native kuberentes方式以session和run-application模式提交任务。

