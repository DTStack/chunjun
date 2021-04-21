package com.dtstack.flinkx.connector.kafka.table;


import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.metrics.MetricConstant;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.io.Serializable;

/** A specific {@link KafkaSerializationSchema} for {@link org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink}. */
class DynamicKafkaSerializationSchema
        implements KafkaSerializationSchema<RowData>, KafkaContextAware<RowData> {

    private static final long serialVersionUID = 1L;

    private final @Nullable FlinkKafkaPartitioner<RowData> partitioner;

    private final String topic;

    private final @Nullable SerializationSchema<RowData> keySerialization;

    private final SerializationSchema<RowData> valueSerialization;

    private final RowData.FieldGetter[] keyFieldGetters;

    private final RowData.FieldGetter[] valueFieldGetters;

    private final boolean hasMetadata;

    private final boolean upsertMode;

    /**
     * consumed row or -1 if this metadata key is not used.
     */
    private final int[] metadataPositions;

    private int[] partitions;

    private int parallelInstanceId;

    private int numParallelInstances;

    protected transient Counter dtNumRecordsOut;

    protected transient Meter dtNumRecordsOutRate;

    DynamicKafkaSerializationSchema(
            String topic,
            @Nullable FlinkKafkaPartitioner<RowData> partitioner,
            @Nullable SerializationSchema<RowData> keySerialization,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] keyFieldGetters,
            RowData.FieldGetter[] valueFieldGetters,
            boolean hasMetadata,
            int[] metadataPositions,
            boolean upsertMode) {
        if (upsertMode) {
            Preconditions.checkArgument(
                    keySerialization != null && keyFieldGetters.length > 0,
                    "Key must be set in upsert mode for serialization schema.");
        }
        this.topic = topic;
        this.partitioner = partitioner;
        this.keySerialization = keySerialization;
        this.valueSerialization = valueSerialization;
        this.keyFieldGetters = keyFieldGetters;
        this.valueFieldGetters = valueFieldGetters;
        this.hasMetadata = hasMetadata;
        this.metadataPositions = metadataPositions;
        this.upsertMode = upsertMode;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        if (keySerialization != null) {
            keySerialization.open(context);
        }
        valueSerialization.open(context);
        if (partitioner != null) {
            partitioner.open(parallelInstanceId, numParallelInstances);
        }
        initMetric(context);
    }

    public void initMetric(SerializationSchema.InitializationContext context) {
        dtNumRecordsOut = context.getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
        dtNumRecordsOutRate = context.getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(dtNumRecordsOut, 20));
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(RowData consumedRow, @Nullable Long timestamp) {
        // shortcut in case no input projection is required
        if (keySerialization == null && !hasMetadata) {
            final byte[] valueSerialized = valueSerialization.serialize(consumedRow);
            dtNumRecordsOut.inc();
            return new ProducerRecord<>(
                    topic,
                    extractPartition(consumedRow, null, valueSerialized),
                    null,
                    valueSerialized);
        }

        final byte[] keySerialized;
        if (keySerialization == null) {
            keySerialized = null;
        } else {
            final RowData keyRow = createProjectedRow(consumedRow, RowKind.INSERT, keyFieldGetters);
            keySerialized = keySerialization.serialize(keyRow);
        }

        final byte[] valueSerialized;
        final RowKind kind = consumedRow.getRowKind();
        final RowData valueRow = createProjectedRow(consumedRow, kind, valueFieldGetters);
        if (upsertMode) {
            if (kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE) {
                // transform the message as the tombstone message
                valueSerialized = null;
            } else {
                // make the message to be INSERT to be compliant with the INSERT-ONLY format
                valueRow.setRowKind(RowKind.INSERT);
                valueSerialized = valueSerialization.serialize(valueRow);
            }
        } else {
            valueSerialized = valueSerialization.serialize(valueRow);
        }

        dtNumRecordsOut.inc();

        return new ProducerRecord<>(
                topic,
                extractPartition(consumedRow, keySerialized, valueSerialized),
                readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.TIMESTAMP),
                keySerialized,
                valueSerialized,
                readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.HEADERS));
    }

    @Override
    public void setParallelInstanceId(int parallelInstanceId) {
        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public void setNumParallelInstances(int numParallelInstances) {
        this.numParallelInstances = numParallelInstances;
    }

    @Override
    public void setPartitions(int[] partitions) {
        this.partitions = partitions;
    }

    @Override
    public String getTargetTopic(RowData element) {
        return topic;
    }

    @SuppressWarnings("unchecked")
    private <T> T readMetadata(RowData consumedRow, KafkaDynamicSink.WritableMetadata metadata) {
        final int pos = metadataPositions[metadata.ordinal()];
        if (pos < 0) {
            return null;
        }
        return (T) metadata.converter.read(consumedRow, pos);
    }

    private Integer extractPartition(
            RowData consumedRow, @Nullable byte[] keySerialized, byte[] valueSerialized) {
        if (partitioner != null) {
            return partitioner.partition(
                    consumedRow, keySerialized, valueSerialized, topic, partitions);
        }
        return null;
    }

    private static RowData createProjectedRow(
            RowData consumedRow, RowKind kind, RowData.FieldGetter[] fieldGetters) {
        final int arity = fieldGetters.length;
        final GenericRowData genericRowData = new GenericRowData(kind, arity);
        for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
            genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
        }
        return genericRowData;
    }

    // --------------------------------------------------------------------------------------------

    interface MetadataConverter extends Serializable {
        Object read(RowData consumedRow, int pos);
    }
}
