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

package com.dtstack.chunjun.connector.kafka.source;

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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** A specific {@link KafkaSerializationSchema} for {@link KafkaDynamicSource}. */
public class DynamicKafkaDeserializationSchema implements KafkaDeserializationSchema<RowData> {

    protected static final Logger LOG =
            LoggerFactory.getLogger(DynamicKafkaDeserializationSchema.class);
    private static final long serialVersionUID = 1L;
    private static final int dataPrintFrequency = 1000;
    private final @Nullable DeserializationSchema<RowData> keyDeserialization;
    private final DeserializationSchema<RowData> valueDeserialization;
    private final boolean hasMetadata;
    private final BufferingCollector keyCollector;
    private final OutputProjectionCollector outputCollector;
    private final TypeInformation<RowData> producedTypeInfo;
    private final boolean upsertMode;
    /** 任务名称 */
    protected String jobName = "defaultJobName";
    /** 任务id */
    protected String jobId;
    /** 任务索引id */
    protected int indexOfSubTask;
    /** 任务开始时间, openInputFormat()开始计算 */
    protected Properties consumerConfig;
    /** 任务开始时间, openInputFormat()开始计算 */
    protected long startTime;
    /** 累加器收集器 */
    protected AccumulatorCollector accumulatorCollector;
    /** 对象大小计算器 */
    protected RowSizeCalculator rowSizeCalculator;
    /** 输入指标组 */
    protected transient BaseMetric inputMetric;
    /** checkpoint状态缓存map */
    protected FormatState formatState;
    /** 统计指标 */
    protected LongCounter numReadCounter;

    protected LongCounter bytesReadCounter;
    protected LongMaximum durationCounter;
    protected LongCounter nReadErrors;

    private transient RuntimeContext runtimeContext;

    protected DirtyManager dirtyManager;

    public DynamicKafkaDeserializationSchema(
            int physicalArity,
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            int[] keyProjection,
            DeserializationSchema<RowData> valueDeserialization,
            int[] valueProjection,
            boolean hasMetadata,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> producedTypeInfo,
            boolean upsertMode) {
        if (upsertMode) {
            Preconditions.checkArgument(
                    keyDeserialization != null && keyProjection.length > 0,
                    "Key must be set in upsert mode for deserialization schema.");
        }
        this.keyDeserialization = keyDeserialization;
        this.valueDeserialization = valueDeserialization;
        this.hasMetadata = hasMetadata;
        this.keyCollector = new BufferingCollector();
        this.outputCollector =
                new OutputProjectionCollector(
                        physicalArity,
                        keyProjection,
                        valueProjection,
                        metadataConverters,
                        upsertMode);
        this.producedTypeInfo = producedTypeInfo;
        this.upsertMode = upsertMode;
    }

    protected void beforeOpen() {
        initRowSizeCalculator();
        initDirtyManager();
        initAccumulatorCollector();
        initStatisticsAccumulator();
        initRestoreInfo();
        openInputFormat();
    }

    private void initDirtyManager() {
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

        ExecutionConfig.GlobalJobParameters params =
                context.getExecutionConfig().getGlobalJobParameters();
        DirtyConfig dc = DirtyConfUtil.parseFromMap(params.toMap());
        this.dirtyManager = new DirtyManager(dc, context);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        beforeOpen();
        if (keyDeserialization != null) {
            keyDeserialization.open(context);
        }
        valueDeserialization.open(context);
        LOG.info(
                "[{}] open successfully, \ninputSplit = {}, \n[{}]: \n{} ",
                this.getClass().getSimpleName(),
                "see other log",
                consumerConfig.getClass().getSimpleName(),
                JsonUtil.toFormatJson(consumerConfig));
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public RowData deserialize(ConsumerRecord<byte[], byte[]> record) {
        throw new IllegalStateException("A collector is required for deserializing.");
    }

    protected void beforeDeserialize(ConsumerRecord<byte[], byte[]> record) {
        if (numReadCounter.getLocalValue() % dataPrintFrequency == 0) {
            if (record != null && record.value() != null) {
                LOG.info(
                        "receive source data:"
                                + new String(record.value(), StandardCharsets.UTF_8));
            } else {
                LOG.info("receive source data, but data is null");
            }
        }

        if (record != null && record.value() != null) {
            updateDuration();
            if (numReadCounter != null) {
                numReadCounter.add(1);
            }
            if (bytesReadCounter != null) {
                bytesReadCounter.add(rowSizeCalculator.getObjectSize(record));
            }
        }
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector)
            throws Exception {
        try {
            beforeDeserialize(record);
            // shortcut in case no output projection is required,
            // also not for a cartesian product with the keys
            if (keyDeserialization == null && !hasMetadata) {
                valueDeserialization.deserialize(record.value(), collector);
                return;
            }

            // buffer key(s)
            if (keyDeserialization != null) {
                keyDeserialization.deserialize(record.key(), keyCollector);
            }

            // project output while emitting values
            outputCollector.inputRecord = record;
            outputCollector.physicalKeyRows = keyCollector.buffer;
            outputCollector.outputCollector = collector;
            if (record.value() == null && upsertMode) {
                // collect tombstone messages in upsert mode by hand
                outputCollector.collect(null);
            } else {
                valueDeserialization.deserialize(record.value(), outputCollector);
            }
            keyCollector.buffer.clear();
        } catch (Exception e) {
            String data = null;
            if (record.value() != null) {
                data = new String(record.value(), StandardCharsets.UTF_8);
            }
            long globalErrors = accumulatorCollector.getAccumulatorValue(Metrics.NUM_ERRORS, false);

            dirtyManager.collect(data, e, null, globalErrors);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public void setRuntimeContext(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    public void setConsumerConfig(Properties consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    /**
     * 更新checkpoint状态缓存map
     *
     * @return
     */
    public FormatState getFormatState() {
        if (formatState != null && numReadCounter != null && inputMetric != null) {
            formatState.setMetric(inputMetric.getMetricCounters());
        }
        return formatState;
    }

    public void setFormatState(FormatState formatState) {
        this.formatState = formatState;
    }

    /** 更新任务执行时间指标 */
    private void updateDuration() {
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
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

    /** 初始化累加器指标 */
    private void initStatisticsAccumulator() {
        numReadCounter = getRuntimeContext().getLongCounter(Metrics.NUM_READS);
        bytesReadCounter = getRuntimeContext().getLongCounter(Metrics.READ_BYTES);
        try {
            durationCounter =
                    (LongMaximum)
                            ReflectionUtils.getDeclaredMethod(
                                            runtimeContext,
                                            "getAccumulator",
                                            String.class,
                                            Class.class)
                                    .invoke(
                                            runtimeContext,
                                            Metrics.READ_DURATION,
                                            LongMaximum.class);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new ChunJunRuntimeException(e);
        }

        inputMetric = new BaseMetric(getRuntimeContext());
        inputMetric.addMetric(Metrics.NUM_READS, numReadCounter, true);
        inputMetric.addMetric(Metrics.READ_BYTES, bytesReadCounter, true);
        inputMetric.addDirtyMetric(Metrics.DIRTY_DATA_COUNT, this.dirtyManager.getConsumedMetric());
        inputMetric.addDirtyMetric(
                Metrics.DIRTY_DATA_COLLECT_FAILED_COUNT,
                this.dirtyManager.getFailedConsumedMetric());
    }

    /** 从checkpoint状态缓存map中恢复上次任务的指标信息 */
    private void initRestoreInfo() {
        if (formatState == null) {
            formatState = new FormatState(indexOfSubTask, null);
        } else {
            numReadCounter.add(formatState.getMetricValue(Metrics.NUM_READS));
            bytesReadCounter.add(formatState.getMetricValue(Metrics.READ_BYTES));
            durationCounter.add(formatState.getMetricValue(Metrics.READ_DURATION));
        }
    }

    private void openInputFormat() {
        Map<String, String> vars = getRuntimeContext().getMetricGroup().getAllVariables();
        if (vars != null) {
            jobName = vars.getOrDefault(Metrics.JOB_NAME, "defaultJobName");
            jobId = vars.get(Metrics.JOB_NAME);
            indexOfSubTask = Integer.parseInt(vars.get(Metrics.SUBTASK_INDEX));
        }

        startTime = System.currentTimeMillis();
    }

    protected void close() {
        if (durationCounter != null) {
            updateDuration();
        }

        if (accumulatorCollector != null) {
            accumulatorCollector.close();
        }

        if (inputMetric != null) {
            inputMetric.waitForReportMetrics();
        }

        LOG.info("subtask input close finished");
    }

    // --------------------------------------------------------------------------------------------

    interface MetadataConverter extends Serializable {
        Object read(ConsumerRecord<?, ?> record);
    }

    // --------------------------------------------------------------------------------------------

    private static final class BufferingCollector implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final List<RowData> buffer = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            buffer.add(record);
        }

        @Override
        public void close() {
            // nothing to do
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Emits a row with key, value, and metadata fields.
     *
     * <p>The collector is able to handle the following kinds of keys:
     *
     * <ul>
     *   <li>No key is used.
     *   <li>A key is used.
     *   <li>The deserialization schema emits multiple keys.
     *   <li>Keys and values have overlapping fields.
     *   <li>Keys are used and value is null.
     * </ul>
     */
    private static final class OutputProjectionCollector
            implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final int physicalArity;

        private final int[] keyProjection;

        private final int[] valueProjection;

        private final MetadataConverter[] metadataConverters;

        private final boolean upsertMode;

        private transient ConsumerRecord<?, ?> inputRecord;

        private transient List<RowData> physicalKeyRows;

        private transient Collector<RowData> outputCollector;

        OutputProjectionCollector(
                int physicalArity,
                int[] keyProjection,
                int[] valueProjection,
                MetadataConverter[] metadataConverters,
                boolean upsertMode) {
            this.physicalArity = physicalArity;
            this.keyProjection = keyProjection;
            this.valueProjection = valueProjection;
            this.metadataConverters = metadataConverters;
            this.upsertMode = upsertMode;
        }

        @Override
        public void collect(RowData physicalValueRow) {
            // no key defined
            if (keyProjection.length == 0) {
                emitRow(null, (GenericRowData) physicalValueRow);
                return;
            }

            // otherwise emit a value for each key
            for (RowData physicalKeyRow : physicalKeyRows) {
                emitRow((GenericRowData) physicalKeyRow, (GenericRowData) physicalValueRow);
            }
        }

        @Override
        public void close() {
            // nothing to do
        }

        private void emitRow(
                @Nullable GenericRowData physicalKeyRow,
                @Nullable GenericRowData physicalValueRow) {
            final RowKind rowKind;
            if (physicalValueRow == null) {
                if (upsertMode) {
                    rowKind = RowKind.DELETE;
                } else {
                    throw new DeserializationException(
                            "Invalid null value received in non-upsert mode. Could not to set row kind for output record.");
                }
            } else {
                rowKind = physicalValueRow.getRowKind();
            }

            final int metadataArity = metadataConverters.length;
            final GenericRowData producedRow =
                    new GenericRowData(rowKind, physicalArity + metadataArity);

            for (int keyPos = 0; keyPos < keyProjection.length; keyPos++) {
                assert physicalKeyRow != null;
                producedRow.setField(keyProjection[keyPos], physicalKeyRow.getField(keyPos));
            }

            if (physicalValueRow != null) {
                for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
                    producedRow.setField(
                            valueProjection[valuePos], physicalValueRow.getField(valuePos));
                }
            }

            for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
                producedRow.setField(
                        physicalArity + metadataPos,
                        metadataConverters[metadataPos].read(inputRecord));
            }

            outputCollector.collect(producedRow);
        }
    }
}
