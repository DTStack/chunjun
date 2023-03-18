package com.dtstack.chunjun.connector.kafka.partitioner;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

/**
 * hash partition key
 *
 * @param <T>
 */
public class CustomerFlinkPartition<T> extends FlinkKafkaPartitioner<T> {
    private static final long serialVersionUID = 1L;
    private int parallelInstanceId;

    public CustomerFlinkPartition() {}

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        Preconditions.checkArgument(
                parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(
                parallelInstances > 0, "Number of subtasks must be larger than 0.");
        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(
                partitions != null && partitions.length > 0,
                "Partitions of the target topic is empty.");
        if (key == null) {
            return partitions[this.parallelInstanceId % partitions.length];
        }
        return partitions[Math.abs(new String(key).hashCode()) % partitions.length];
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof CustomerFlinkPartition;
    }

    @Override
    public int hashCode() {
        return CustomerFlinkPartition.class.hashCode();
    }
}
