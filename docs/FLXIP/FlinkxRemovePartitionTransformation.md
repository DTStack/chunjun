
## Flinkx 关于移除 PartitionTransformation
1. Main程序在SourceTransformation与SinkTransformation之间加入了PartitionTransformation（Partitioner：DTRebalancePartitioner）

	```java
	...
	
	DataReader dataReader = DataReaderFactory.getDataReader(config, env);
	DataStream<Row> dataStream = dataReader.readData();

	dataStream = new DataStream<>(dataStream.getExecutionEnvironment(),
                new PartitionTransformation<>(dataStream.getTransformation(),
                        new DTRebalancePartitioner<>()));

	DataWriter dataWriter = DataWriterFactory.getDataWriter(config);
	dataWriter.writeData(dataStream);
	
	...//省略前后代码
	
	```
2. 在构建StreamGraph时，SinkTransformation与SourceTransformation会形成2个StreamNode（每个StreamNode包含一个算子Operator）与1个StreamEdge，因为DTRebalancePartitioner的原因，StreamEdge的Partition属性不等于ForwardPartitioner
3. 在构建JobGraph是，Flink 通过 StreamingJobGraphGenerator#isChainable 判断 Operator 能否被chain到同一个 JobVertex 中
	
	```java
	
	public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
		StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
		StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

		StreamOperator<?> headOperator = upStreamVertex.getOperator();
		StreamOperator<?> outOperator = downStreamVertex.getOperator();

		return downStreamVertex.getInEdges().size() == 1
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
					headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& streamGraph.isChainingEnabled();
	}
	
	```

4. 显然，此时因为Partitioner条件不满足，source与sink两个Operator算子不会被chain在同一个JobVertex中
5. 紧接着任务提交到JobManager中执行，在构建ExecutionGraph时，两个Operator算子最后就会生成2个ExecutionJobVertex，再根据Operator的 parallelism 生成对应数量的 ExecutionVertex，一个ExecutionVertex对应一个最终执行的Task任务
6. 最差的情况，因为Operator无法被chain的原因，同一个job的两个Task任务有可能在不同节点的TaskExecutor的Slot上执行，此时数据传输的效率最低。

##### 结论：Main函数因为历史原因加入了 Partition，之前有一些累加器的数值采集的是两个vertx之间的数据，如果只有一个vertx的情况下，累加器的数值为空。  <br/>目前 Flinkx 经过几番迭代后，根据自测结果，程序移除 Partition 并不影响 Accumulator 的统计数值。<br/>如果程序没有其他对 Partition 的依赖项，就可以移除 PartitionTransformation 让算子chain。


```java

//测试代码
@Test
public void testFlinkxTransformationn() throws Exception {

    Set<Long> set = new HashSet<>(1);
    set.add(1L);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    CollectionInputFormat<Long> inputFormat = new CollectionInputFormat<Long>(set, LongSerializer.INSTANCE);
    TypeInformation typeInfo = TypeInformation.of(Long.class);
    InputFormatSourceFunction<Long> inputFormatSourceFunction = new InputFormatSourceFunction<Long>(inputFormat, typeInfo);
    DataStream<Long> sourceStream = env.addSource(inputFormatSourceFunction, "source", typeInfo);

    
    //      sourceStream = new DataStream<>(sourceStream.getExecutionEnvironment(),
    //          new PartitionTransformation<>(sourceStream.getTransformation(),
    //              new RebalancePartitioner<>()));


    PrintingOutputFormat printingOutputFormat = new PrintingOutputFormat();
    OutputFormatSinkFunction<Long> outputFormatSinkFunction = new OutputFormatSinkFunction<Long>(printingOutputFormat);

    sourceStream.addSink(outputFormatSinkFunction);
    
    
    //        assertEquals(2, env.getStreamGraph().getJobGraph().getNumberOfVertices());
    assertEquals(1, env.getStreamGraph().getJobGraph().getNumberOfVertices());
}
```

Ps：SlotSharingGroup 与 CoLocationGroup 属性可优化算子子任务的分配方式
