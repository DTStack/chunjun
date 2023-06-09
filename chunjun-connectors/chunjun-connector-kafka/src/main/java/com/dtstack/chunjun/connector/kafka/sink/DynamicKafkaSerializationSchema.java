/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.kafka.sink;

import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.dirty.manager.DirtyManager;
import com.dtstack.chunjun.dirty.utils.DirtyConfUtil;
import com.dtstack.chunjun.metrics.AccumulatorCollector;
import com.dtstack.chunjun.metrics.BaseMetric;
import com.dtstack.chunjun.metrics.RowSizeCalculator;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.ReflectionUtils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.Properties;

/**
 * A specific {@link KafkaSerializationSchema} for {@link
 * org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink}.
 */
public class DynamicKafkaSerializationSchema
        implements KafkaSerializationSchema<RowData>, KafkaContextAware<RowData> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicKafkaSerializationSchema.class);

    private static final long serialVersionUID = 1L;

    protected final @Nullable FlinkKafkaPartitioner<RowData> partitioner;

    protected final String topic;

    private final @Nullable SerializationSchema<RowData> keySerialization;

    private final SerializationSchema<RowData> valueSerialization;

    private final RowData.FieldGetter[] keyFieldGetters;

    private final RowData.FieldGetter[] valueFieldGetters;

    private final boolean hasMetadata;

    private final boolean upsertMode;

    /** consumed row or -1 if this metadata key is not used. */
    private final int[] metadataPositions;

    protected int parallelInstanceId;
    protected int numParallelInstances;
    protected Properties producerConfig;
    /** 虽然开启cp，是否采用定时器和一定条数让下游数据可见。 EXACTLY_ONCE：否，遵循两阶段提交协议。 AT_LEAST_ONCE：是，只要数据条数或者到达定时时间即可见 */
    protected CheckpointingMode checkpointMode;
    /** 任务开始时间, openInputFormat()开始计算 */
    protected long startTime;
    /** 是否开启了checkpoint */
    protected boolean checkpointEnabled;
    /** 输出指标组 */
    protected transient BaseMetric outputMetric;
    /** checkpoint状态缓存map */
    protected FormatState formatState;
    /** 累加器收集器 */
    protected AccumulatorCollector accumulatorCollector;
    /** 对象大小计算器 */
    protected RowSizeCalculator rowSizeCalculator;

    protected LongCounter bytesWriteCounter;
    protected LongMaximum durationCounter;
    protected LongCounter numWriteCounter;
    protected LongCounter snapshotWriteCounter;
    protected LongCounter errCounter;
    protected LongCounter nullErrCounter;
    protected LongCounter duplicateErrCounter;
    protected LongCounter conversionErrCounter;
    protected LongCounter otherErrCounter;
    protected LongCounter nWriteErrors;
    protected LongCounter errBytes;
    private int[] partitions;
    private transient RuntimeContext runtimeContext;

    protected DirtyManager dirtyManager;

    public DynamicKafkaSerializationSchema(
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

    private static RowData createProjectedRow(
            RowData consumedRow, RowKind kind, RowData.FieldGetter[] fieldGetters) {
        final int arity = fieldGetters.length;
        final GenericRowData genericRowData = new GenericRowData(kind, arity);
        for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
            genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
        }
        return genericRowData;
    }

    protected void beforeOpen() {
        StreamingRuntimeContext context = (StreamingRuntimeContext) this.runtimeContext;
        this.checkpointEnabled = context.isCheckpointingEnabled();
        this.startTime = System.currentTimeMillis();

        ExecutionConfig.GlobalJobParameters params =
                context.getExecutionConfig().getGlobalJobParameters();
        DirtyConfig dc = DirtyConfUtil.parseFromMap(params.toMap());

        initRowSizeCalculator();
        this.dirtyManager = new DirtyManager(dc, context);

        initStatisticsAccumulator();
        initRestoreInfo();
        initAccumulatorCollector();

        checkpointMode =
                context.getCheckpointMode() == null
                        ? CheckpointingMode.AT_LEAST_ONCE
                        : context.getCheckpointMode();

        if (partitioner != null) {
            partitioner.open(
                    this.runtimeContext.getIndexOfThisSubtask(),
                    this.runtimeContext.getNumberOfParallelSubtasks());
        }
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        beforeOpen();
        if (keySerialization != null) {
            keySerialization.open(context);
        }
        valueSerialization.open(context);
        LOG.info(
                "[{}] open successfully, \ncheckpointMode = {}, \ncheckpointEnabled = {}, \nflushIntervalMills = {}, \nbatchSize = {}, \n[{}]: \n{} ",
                this.getClass().getSimpleName(),
                checkpointMode,
                checkpointEnabled,
                0,
                1,
                producerConfig.getClass().getSimpleName(),
                JsonUtil.toFormatJson(producerConfig));
    }

    /**
     * 指标更新
     *
     * @param size
     * @param rowData
     */
    protected void beforeSerialize(long size, RowData rowData) {
        updateDuration();
        numWriteCounter.add(size);
        bytesWriteCounter.add(rowSizeCalculator.getObjectSize(rowData));
        if (checkpointEnabled) {
            snapshotWriteCounter.add(size);
        }
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(RowData consumedRow, @Nullable Long timestamp) {
        try {
            beforeSerialize(1, consumedRow);
            // shortcut in case no input projection is required
            if (keySerialization == null && !hasMetadata) {
                final byte[] valueSerialized = valueSerialization.serialize(consumedRow);
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
                final RowData keyRow =
                        createProjectedRow(consumedRow, RowKind.INSERT, keyFieldGetters);
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

            return new ProducerRecord<>(
                    topic,
                    extractPartition(consumedRow, keySerialized, valueSerialized),
                    readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.TIMESTAMP),
                    keySerialized,
                    valueSerialized,
                    readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.HEADERS));
        } catch (Exception e) {
            long globalErrors = accumulatorCollector.getAccumulatorValue(Metrics.NUM_ERRORS, false);
            dirtyManager.collect(consumedRow, e, null, globalErrors);
        }
        return null;
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

    protected Integer extractPartition(
            RowData consumedRow, @Nullable byte[] keySerialized, byte[] valueSerialized) {
        if (partitioner != null) {
            return partitioner.partition(
                    consumedRow, keySerialized, valueSerialized, topic, partitions);
        }
        return null;
    }

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public void setRuntimeContext(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    public void setProducerConfig(Properties producerConfig) {
        this.producerConfig = producerConfig;
    }

    public void close() {
        if (durationCounter != null) {
            updateDuration();
        }

        if (outputMetric != null) {
            outputMetric.waitForReportMetrics();
        }

        if (accumulatorCollector != null) {
            accumulatorCollector.close();
        }

        LOG.info("subtask output close finished");
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 更新checkpoint状态缓存map
     *
     * @return
     */
    public FormatState getFormatState() {
        formatState.setNumberWrite(numWriteCounter.getLocalValue());
        formatState.setMetric(outputMetric.getMetricCounters());
        LOG.info("format state:{}", formatState.getState());
        return formatState;
    }

    public void setFormatState(FormatState formatState) {
        this.formatState = formatState;
    }

    /** 初始化累加器指标 */
    private void initStatisticsAccumulator() {
        errCounter = runtimeContext.getLongCounter(Metrics.NUM_ERRORS);
        nullErrCounter = runtimeContext.getLongCounter(Metrics.NUM_NULL_ERRORS);
        duplicateErrCounter = runtimeContext.getLongCounter(Metrics.NUM_DUPLICATE_ERRORS);
        conversionErrCounter = runtimeContext.getLongCounter(Metrics.NUM_CONVERSION_ERRORS);
        otherErrCounter = runtimeContext.getLongCounter(Metrics.NUM_OTHER_ERRORS);
        numWriteCounter = runtimeContext.getLongCounter(Metrics.NUM_WRITES);
        snapshotWriteCounter = runtimeContext.getLongCounter(Metrics.SNAPSHOT_WRITES);
        bytesWriteCounter = runtimeContext.getLongCounter(Metrics.WRITE_BYTES);
        try {
            durationCounter =
                    (LongMaximum)
                            Objects.requireNonNull(
                                            ReflectionUtils.getDeclaredMethod(
                                                    runtimeContext,
                                                    "getAccumulator",
                                                    String.class,
                                                    Class.class))
                                    .invoke(
                                            runtimeContext,
                                            Metrics.WRITE_DURATION,
                                            LongMaximum.class);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new ChunJunRuntimeException(e);
        }

        outputMetric = new BaseMetric(runtimeContext);
        outputMetric.addMetric(Metrics.NUM_ERRORS, errCounter);
        outputMetric.addMetric(Metrics.NUM_NULL_ERRORS, nullErrCounter);
        outputMetric.addMetric(Metrics.NUM_DUPLICATE_ERRORS, duplicateErrCounter);
        outputMetric.addMetric(Metrics.NUM_CONVERSION_ERRORS, conversionErrCounter);
        outputMetric.addMetric(Metrics.NUM_OTHER_ERRORS, otherErrCounter);
        outputMetric.addMetric(Metrics.NUM_WRITES, numWriteCounter, true);
        outputMetric.addMetric(Metrics.SNAPSHOT_WRITES, snapshotWriteCounter);
        outputMetric.addMetric(Metrics.WRITE_BYTES, bytesWriteCounter, true);
        outputMetric.addDirtyMetric(
                Metrics.DIRTY_DATA_COUNT, this.dirtyManager.getConsumedMetric());
        outputMetric.addDirtyMetric(
                Metrics.DIRTY_DATA_COLLECT_FAILED_COUNT,
                this.dirtyManager.getFailedConsumedMetric());
    }

    /** 初始化累加器收集器 */
    private void initAccumulatorCollector() {
        accumulatorCollector =
                new AccumulatorCollector(
                        (StreamingRuntimeContext) runtimeContext, Metrics.METRIC_SINK_LIST);
        accumulatorCollector.start();
    }

    /** 初始化对象大小计算器 */
    private void initRowSizeCalculator() {
        rowSizeCalculator = RowSizeCalculator.getRowSizeCalculator();
    }

    /** 从checkpoint状态缓存map中恢复上次任务的指标信息 */
    private void initRestoreInfo() {
        if (formatState == null) {
            formatState = new FormatState(runtimeContext.getIndexOfThisSubtask(), null);
        } else {
            errCounter.add(formatState.getMetricValue(Metrics.NUM_ERRORS));
            nullErrCounter.add(formatState.getMetricValue(Metrics.NUM_NULL_ERRORS));
            duplicateErrCounter.add(formatState.getMetricValue(Metrics.NUM_DUPLICATE_ERRORS));
            conversionErrCounter.add(formatState.getMetricValue(Metrics.NUM_CONVERSION_ERRORS));
            otherErrCounter.add(formatState.getMetricValue(Metrics.NUM_OTHER_ERRORS));
            numWriteCounter.add(formatState.getMetricValue(Metrics.NUM_WRITES));
            snapshotWriteCounter.add(formatState.getMetricValue(Metrics.SNAPSHOT_WRITES));
            bytesWriteCounter.add(formatState.getMetricValue(Metrics.WRITE_BYTES));
            durationCounter.add(formatState.getMetricValue(Metrics.WRITE_DURATION));
        }
    }

    /** 更新任务执行时间指标 */
    private void updateDuration() {
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    interface MetadataConverter extends Serializable {
        Object read(RowData consumedRow, int pos);
    }
}
