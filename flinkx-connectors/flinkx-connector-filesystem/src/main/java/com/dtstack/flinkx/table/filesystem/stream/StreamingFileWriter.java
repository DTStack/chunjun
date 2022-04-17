package com.dtstack.flinkx.table.filesystem.stream;

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

import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.dirty.DirtyConf;
import com.dtstack.flinkx.dirty.manager.DirtyManager;
import com.dtstack.flinkx.dirty.utils.DirtyConfUtil;
import com.dtstack.flinkx.metrics.AccumulatorCollector;
import com.dtstack.flinkx.metrics.BaseMetric;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.streaming.api.functions.sink.filesystem.StreamingFileSink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.stream.PartitionCommitInfo;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/** Writer for emitting {@link PartitionCommitInfo} to downstream. */
public class StreamingFileWriter<IN> extends AbstractStreamingWriter<IN, PartitionCommitInfo> {

    private static final long serialVersionUID = 2L;

    private transient Set<String> currentNewPartitions;
    private transient TreeMap<Long, Set<String>> newPartitions;
    private transient Set<String> committablePartitions;

    protected transient ListState<FormatState> unionOffsetStates;
    protected Map<Integer, FormatState> formatStateMap;
    protected FormatState formatState;

    protected CheckpointingMode checkpointMode;

    protected long startTime;

    /** 是否开启了checkpoint */
    protected boolean checkpointEnabled;

    private static final Logger LOG = LoggerFactory.getLogger(StreamingFileSink.class);

    /** 输出指标组 */
    protected transient BaseMetric outputMetric;

    protected AccumulatorCollector accumulatorCollector;

    protected LongCounter bytesWriteCounter;
    protected LongCounter durationCounter;
    protected LongCounter numWriteCounter;
    protected LongCounter snapshotWriteCounter;
    protected LongCounter errCounter;
    protected LongCounter nullErrCounter;
    protected LongCounter duplicateErrCounter;
    protected LongCounter conversionErrCounter;
    protected LongCounter otherErrCounter;

    protected DirtyManager dirtyManager;

    protected static final String LOCATION_STATE_NAME = "data-sync-location-states";

    public StreamingFileWriter(
            long bucketCheckInterval,
            StreamingFileSink.BucketsBuilder<
                            IN, String, ? extends StreamingFileSink.BucketsBuilder<IN, String, ?>>
                    bucketsBuilder) {
        super(bucketCheckInterval, bucketsBuilder);
    }

    public FormatState getFormatState() {
        formatState.setNumberWrite(numWriteCounter.getLocalValue());
        formatState.setMetric(outputMetric.getMetricCounters());
        return formatState;
    }

    /** 初始化累加器收集器 */
    private void initAccumulatorCollector() {
        accumulatorCollector =
                new AccumulatorCollector(
                        (StreamingRuntimeContext) getRuntimeContext(), Metrics.METRIC_SINK_LIST);
        accumulatorCollector.start();
    }

    private void initRestoreInfo() {
        if (formatState == null) {
            formatState = new FormatState(getRuntimeContext().getIndexOfThisSubtask(), null);
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

    /** 初始化累加器指标 */
    private void initStatisticsAccumulator() {
        errCounter = getRuntimeContext().getLongCounter(Metrics.NUM_ERRORS);
        nullErrCounter = getRuntimeContext().getLongCounter(Metrics.NUM_NULL_ERRORS);
        duplicateErrCounter = getRuntimeContext().getLongCounter(Metrics.NUM_DUPLICATE_ERRORS);
        conversionErrCounter = getRuntimeContext().getLongCounter(Metrics.NUM_CONVERSION_ERRORS);
        otherErrCounter = getRuntimeContext().getLongCounter(Metrics.NUM_OTHER_ERRORS);
        numWriteCounter = getRuntimeContext().getLongCounter(Metrics.NUM_WRITES);
        snapshotWriteCounter = getRuntimeContext().getLongCounter(Metrics.SNAPSHOT_WRITES);
        bytesWriteCounter = getRuntimeContext().getLongCounter(Metrics.WRITE_BYTES);
        durationCounter = getRuntimeContext().getLongCounter(Metrics.WRITE_DURATION);

        outputMetric = new BaseMetric(getRuntimeContext());
        outputMetric.addMetric(Metrics.NUM_ERRORS, errCounter);
        outputMetric.addMetric(Metrics.NUM_NULL_ERRORS, nullErrCounter);
        outputMetric.addMetric(Metrics.NUM_DUPLICATE_ERRORS, duplicateErrCounter);
        outputMetric.addMetric(Metrics.NUM_CONVERSION_ERRORS, conversionErrCounter);
        outputMetric.addMetric(Metrics.NUM_OTHER_ERRORS, otherErrCounter);
        outputMetric.addMetric(Metrics.NUM_WRITES, numWriteCounter, true);
        outputMetric.addMetric(Metrics.SNAPSHOT_WRITES, snapshotWriteCounter);
        outputMetric.addMetric(Metrics.WRITE_BYTES, bytesWriteCounter, true);
        outputMetric.addMetric(Metrics.WRITE_DURATION, durationCounter);
        outputMetric.addDirtyMetric(
                Metrics.DIRTY_DATA_COUNT, this.dirtyManager.getConsumedMetric());
        outputMetric.addDirtyMetric(
                Metrics.DIRTY_DATA_COLLECT_FAILED_COUNT,
                this.dirtyManager.getFailedConsumedMetric());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        LOG.info("Start initialize output format state");
        this.startTime = System.currentTimeMillis();
        OperatorStateStore stateStore = context.getOperatorStateStore();
        unionOffsetStates =
                stateStore.getUnionListState(
                        new ListStateDescriptor<>(
                                LOCATION_STATE_NAME,
                                TypeInformation.of(new TypeHint<FormatState>() {})));
        LOG.info("Is restored:{}", context.isRestored());
        if (context.isRestored()) {
            formatStateMap = new HashMap<>(16);
            for (FormatState formatState : unionOffsetStates.get()) {
                formatStateMap.put(formatState.getNumOfSubTask(), formatState);
                LOG.info("Output format state into:{}", formatState);
            }
        }
        if (formatStateMap != null) {
            this.formatState = formatStateMap.get(getRuntimeContext().getIndexOfThisSubtask());
        }
        LOG.info("End initialize output format state");
        StreamingRuntimeContext streamingRuntimeContext =
                (StreamingRuntimeContext) getRuntimeContext();
        checkpointMode =
                streamingRuntimeContext.getCheckpointMode() == null
                        ? CheckpointingMode.AT_LEAST_ONCE
                        : streamingRuntimeContext.getCheckpointMode();
        this.checkpointEnabled = streamingRuntimeContext.isCheckpointingEnabled();
        ExecutionConfig.GlobalJobParameters params =
                streamingRuntimeContext.getExecutionConfig().getGlobalJobParameters();
        DirtyConf dc = DirtyConfUtil.parseFromMap(params.toMap());
        this.dirtyManager = new DirtyManager(dc, streamingRuntimeContext);
        initStatisticsAccumulator();
        initRestoreInfo();
        initAccumulatorCollector();
        currentNewPartitions = new HashSet<>();
        newPartitions = new TreeMap<>();
        committablePartitions = new HashSet<>();
        super.initializeState(context);
    }

    @Override
    protected void partitionCreated(String partition) {
        currentNewPartitions.add(partition);
    }

    @Override
    protected void partitionInactive(String partition) {
        committablePartitions.add(partition);
    }

    @Override
    protected void onPartFileOpened(String s, Path newPath) {}

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        newPartitions.put(context.getCheckpointId(), new HashSet<>(currentNewPartitions));
        currentNewPartitions.clear();
        FormatState formatState = getFormatState();
        if (formatState != null) {
            LOG.info("OutputFormat format state:{}", formatState);
            unionOffsetStates.clear();
            unionOffsetStates.add(formatState);
        }
    }

    protected void beforeSerialize(long size, RowData rowData) {
        updateDuration();
        numWriteCounter.add(size);
        bytesWriteCounter.add(ObjectSizeCalculator.getObjectSize(rowData));
        if (checkpointEnabled) {
            snapshotWriteCounter.add(size);
        }
    }

    /** 更新任务执行时间指标 */
    private void updateDuration() {
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    @Override
    protected void commitUpToCheckpoint(long checkpointId) throws Exception {
        super.commitUpToCheckpoint(checkpointId);

        NavigableMap<Long, Set<String>> headPartitions =
                this.newPartitions.headMap(checkpointId, true);
        Set<String> partitions = new HashSet<>(committablePartitions);
        committablePartitions.clear();
        headPartitions.values().forEach(partitions::addAll);
        headPartitions.clear();

        output.collect(
                new StreamRecord<>(
                        new PartitionCommitInfo(
                                checkpointId,
                                getRuntimeContext().getIndexOfThisSubtask(),
                                getRuntimeContext().getNumberOfParallelSubtasks(),
                                new ArrayList<>(partitions))));
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        beforeSerialize(1, (RowData) element.getValue());
        super.processElement(element);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (outputMetric != null) {
            outputMetric.waitForReportMetrics();
        }
    }
}
