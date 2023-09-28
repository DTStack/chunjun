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

package com.dtstack.chunjun.connector.rocketmq.source.deserialization;

import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.dirty.manager.DirtyManager;
import com.dtstack.chunjun.dirty.utils.DirtyConfUtil;
import com.dtstack.chunjun.metrics.AccumulatorCollector;
import com.dtstack.chunjun.metrics.BaseMetric;
import com.dtstack.chunjun.restore.FormatState;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.Arrays;

/**
 * * The row based implementation of {@link KeyValueDeserializationSchema} for the deserialization
 * of message key and value..
 */
public class RowKeyValueDeserializationSchema implements KeyValueDeserializationSchema<RowData> {

    private static final long serialVersionUID = -6942104006005258601L;

    private final transient ResolvedSchema tableSchema;

    protected FormatState formatState;

    protected transient BaseMetric inputMetric;
    protected long startTime;
    protected LongCounter numReadCounter;
    protected LongCounter bytesReadCounter;
    protected LongCounter durationCounter;
    // dirty data
    protected DirtyManager dirtyManager;
    // gateway
    protected AccumulatorCollector accumulatorCollector;

    private transient RuntimeContext runtimeContext;
    private final AbstractRowConverter converter;

    public RowKeyValueDeserializationSchema(
            ResolvedSchema tableSchema, AbstractRowConverter converter) {
        this.tableSchema = tableSchema;
        this.converter = converter;
    }

    @Override
    public RowData deserializeKeyAndValue(byte[] key, byte[] value) {
        beforeDeserialize(value);
        return deserializeValue(value);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void init() {
        initDirtyManager();
        initAccumulatorCollector();
        initStatisticsAccumulator();
        initRestoreInfo();
        startTime = System.currentTimeMillis();
    }

    @Override
    public void close() {
        if (durationCounter != null) {
            updateDuration();
        }

        if (accumulatorCollector != null) {
            accumulatorCollector.close();
        }

        if (inputMetric != null) {
            inputMetric.waitForReportMetrics();
        }
    }

    protected void beforeDeserialize(byte[] value) {
        if (value != null) {
            updateDuration();
            if (numReadCounter != null) {
                numReadCounter.add(1L);
            }
            if (bytesReadCounter != null) {
                bytesReadCounter.add(value.length);
            }
        }
    }

    /** 更新任务执行时间指标 */
    private void updateDuration() {
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    private void initDirtyManager() {
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

        ExecutionConfig.GlobalJobParameters params =
                context.getExecutionConfig().getGlobalJobParameters();
        DirtyConfig dc = DirtyConfUtil.parseFromMap(params.toMap());
        this.dirtyManager = new DirtyManager(dc, context);
    }

    /** 初始化累加器收集器 */
    private void initAccumulatorCollector() {
        String lastWriteLocation =
                String.format(
                        "%s_%s",
                        Metrics.LAST_WRITE_LOCATION_PREFIX, runtimeContext.getIndexOfThisSubtask());
        String lastWriteNum =
                String.format(
                        "%s_%s",
                        Metrics.LAST_WRITE_NUM__PREFIX, runtimeContext.getIndexOfThisSubtask());

        accumulatorCollector =
                new AccumulatorCollector(
                        (StreamingRuntimeContext) runtimeContext,
                        Arrays.asList(
                                Metrics.NUM_READS,
                                Metrics.READ_BYTES,
                                Metrics.READ_DURATION,
                                Metrics.WRITE_BYTES,
                                Metrics.NUM_WRITES,
                                lastWriteLocation,
                                lastWriteNum));
        accumulatorCollector.start();
    }

    /** 初始化累加器指标 */
    private void initStatisticsAccumulator() {
        numReadCounter = getRuntimeContext().getLongCounter(Metrics.NUM_READS);
        bytesReadCounter = getRuntimeContext().getLongCounter(Metrics.READ_BYTES);
        durationCounter = getRuntimeContext().getLongCounter(Metrics.READ_DURATION);

        inputMetric = new BaseMetric(getRuntimeContext());
        inputMetric.addMetric(Metrics.NUM_READS, numReadCounter, true);
        inputMetric.addMetric(Metrics.READ_BYTES, bytesReadCounter, true);
        inputMetric.addMetric(Metrics.READ_DURATION, durationCounter);
        inputMetric.addDirtyMetric(Metrics.DIRTY_DATA_COUNT, this.dirtyManager.getConsumedMetric());
        inputMetric.addDirtyMetric(
                Metrics.DIRTY_DATA_COLLECT_FAILED_COUNT,
                this.dirtyManager.getFailedConsumedMetric());
    }

    /** 从checkpoint状态缓存map中恢复上次任务的指标信息 */
    private void initRestoreInfo() {
        if (formatState == null) {
            formatState = new FormatState(runtimeContext.getIndexOfThisSubtask(), null);
        } else {
            numReadCounter.add(formatState.getMetricValue(Metrics.NUM_READS));
            bytesReadCounter.add(formatState.getMetricValue(Metrics.READ_BYTES));
            durationCounter.add(formatState.getMetricValue(Metrics.READ_DURATION));
        }
    }

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public void setRuntimeContext(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    public FormatState getFormatState() {
        if (formatState != null && numReadCounter != null && inputMetric != null) {
            formatState.setMetric(inputMetric.getMetricCounters());
        }
        return formatState;
    }

    public void setFormatState(FormatState formatState) {
        this.formatState = formatState;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(tableSchema.toPhysicalRowDataType().getLogicalType());
    }

    private RowData deserializeValue(byte[] value) {
        try {
            return converter.toInternal(value);
        } catch (Exception e) {
            long globalErrors = accumulatorCollector.getAccumulatorValue(Metrics.NUM_ERRORS, false);
            dirtyManager.collect(value, e, null, globalErrors);
        }
        return null;
    }

    /** Builder of {@link RowKeyValueDeserializationSchema}. */
    public static class Builder {

        private ResolvedSchema schema;
        private AbstractRowConverter converter;

        public Builder() {}

        public Builder setConverter(AbstractRowConverter converter) {
            this.converter = converter;
            return this;
        }

        public Builder setTableSchema(ResolvedSchema tableSchema) {
            this.schema = tableSchema;
            return this;
        }

        public RowKeyValueDeserializationSchema build() {
            return new RowKeyValueDeserializationSchema(schema, converter);
        }
    }
}
