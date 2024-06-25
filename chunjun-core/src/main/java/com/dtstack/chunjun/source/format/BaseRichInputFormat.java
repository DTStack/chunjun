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

package com.dtstack.chunjun.source.format;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.dirty.manager.DirtyManager;
import com.dtstack.chunjun.dirty.utils.DirtyConfUtil;
import com.dtstack.chunjun.metrics.AccumulatorCollector;
import com.dtstack.chunjun.metrics.BaseMetric;
import com.dtstack.chunjun.metrics.CustomReporter;
import com.dtstack.chunjun.metrics.RowSizeCalculator;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.ByteRateLimiter;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.DataSyncFactoryUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ChunJun里面所有自定义inputFormat的抽象基类
 *
 * <p>扩展了org.apache.flink.api.common.io.RichInputFormat, 因而可以通过{@link
 * #getRuntimeContext()}获取运行时执行上下文 自动完成 用户只需覆盖openInternal,closeInternal等方法, 无需操心细节
 */
@Slf4j
@Getter
@Setter
public abstract class BaseRichInputFormat extends RichInputFormat<RowData, InputSplit> {

    private static final long serialVersionUID = -7071023353207658370L;

    /** BaseRichInputFormat是否结束 */
    protected final AtomicBoolean isClosed = new AtomicBoolean(false);
    /** 环境上下文 */
    protected StreamingRuntimeContext context;
    /** 任务名称 */
    protected String jobName = "defaultJobName";
    /** 任务id */
    protected String jobId;
    /** 任务索引id */
    protected int indexOfSubTask;
    /** 任务开始时间, openInputFormat()开始计算 */
    protected long startTime;
    /** 任务公共配置 */
    protected CommonConfig config;
    /** 数据类型转换器 */
    protected AbstractRowConverter rowConverter;
    /** 输入指标组 */
    protected transient BaseMetric inputMetric;
    /** 自定义的prometheus reporter，用于提交startLocation和endLocation指标 */
    protected transient CustomReporter customReporter;
    /** 累加器收集器 */
    protected AccumulatorCollector accumulatorCollector;
    /** 对象大小计算器 */
    protected RowSizeCalculator rowSizeCalculator;
    /** checkpoint状态缓存map */
    protected FormatState formatState;

    protected LongCounter numReadCounter;
    protected LongCounter bytesReadCounter;
    protected LongCounter durationCounter;
    protected ByteRateLimiter byteRateLimiter;
    /** A collection of field names filled in user scripts with constants removed */
    protected List<String> columnNameList = new ArrayList<>();
    /** A collection of field types filled in user scripts with constants removed */
    protected List<TypeConfig> columnTypeList = new ArrayList<>();
    /** dirty manager which collects the dirty data. */
    protected DirtyManager dirtyManager;
    /** BaseRichInputFormat是否已经初始化 */
    private boolean initialized = false;

    protected boolean useAbstractColumn;

    @Override
    public final void configure(Configuration parameters) {
        // do nothing
    }

    @Override
    public final BaseStatistics getStatistics(BaseStatistics baseStatistics) {
        return null;
    }

    @Override
    public final InputSplit[] createInputSplits(int minNumSplits) {
        try {
            return createInputSplitsInternal(minNumSplits);
        } catch (Exception e) {
            log.warn("error to create InputSplits", e);
            return new ErrorInputSplit[] {new ErrorInputSplit(ExceptionUtil.getErrorMessage(e))};
        }
    }

    @Override
    public final InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        this.context = (StreamingRuntimeContext) getRuntimeContext();

        ExecutionConfig.GlobalJobParameters params =
                context.getExecutionConfig().getGlobalJobParameters();
        DirtyConfig dc = DirtyConfUtil.parseFromMap(params.toMap());
        this.dirtyManager = new DirtyManager(dc, this.context);

        if (inputSplit instanceof ErrorInputSplit) {
            throw new RuntimeException(((ErrorInputSplit) inputSplit).getErrorMessage());
        }

        if (!initialized) {
            initAccumulatorCollector();
            initRowSizeCalculator();
            initStatisticsAccumulator();
            initByteRateLimiter();
            initRestoreInfo();
            initialized = true;
        }

        openInternal(inputSplit);

        log.info(
                "[{}] open successfully, \ninputSplit = {}, \n[{}]: \n{} ",
                this.getClass().getSimpleName(),
                inputSplit,
                config.getClass().getSimpleName(),
                JsonUtil.toPrintJson(config));
    }

    @Override
    public void openInputFormat() throws IOException {
        Map<String, String> vars = getRuntimeContext().getMetricGroup().getAllVariables();
        if (vars != null) {
            jobName = vars.getOrDefault(Metrics.JOB_NAME, "defaultJobName");
            jobId = vars.get(Metrics.JOB_NAME);
            indexOfSubTask = Integer.parseInt(vars.get(Metrics.SUBTASK_INDEX));
        }

        if (useCustomReporter()) {
            customReporter =
                    DataSyncFactoryUtil.discoverMetric(
                            config, getRuntimeContext(), makeTaskFailedWhenReportFailed());
            customReporter.open();
        }

        startTime = System.currentTimeMillis();
    }

    @Override
    public RowData nextRecord(RowData rowData) throws ReadRecordException {
        if (byteRateLimiter != null) {
            byteRateLimiter.acquire();
        }
        RowData internalRow = nextRecordInternal(rowData);
        if (internalRow != null) {
            updateDuration();
            if (numReadCounter != null) {
                numReadCounter.add(1);
            }
            if (bytesReadCounter != null) {
                bytesReadCounter.add(rowSizeCalculator.getObjectSize(internalRow));
            }
        }

        return internalRow;
    }

    @Override
    public void close() throws IOException {
        closeInternal();

        if (dirtyManager != null) {
            dirtyManager.close();
        }
    }

    @Override
    public void closeInputFormat() {
        if (isClosed.get()) {
            return;
        }

        updateDuration();

        if (byteRateLimiter != null) {
            byteRateLimiter.stop();
        }

        if (accumulatorCollector != null) {
            accumulatorCollector.close();
        }

        if (useCustomReporter() && null != customReporter) {
            customReporter.report();
        }

        if (inputMetric != null) {
            inputMetric.waitForReportMetrics();
        }

        if (useCustomReporter() && null != customReporter) {
            customReporter.close();
        }

        isClosed.set(true);
        log.info("subtask input close finished");
    }

    /** 更新任务执行时间指标 */
    protected void updateDuration() {
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    /** 初始化累加器收集器 */
    private void initAccumulatorCollector() {
        String lastWriteLocation =
                String.format("%s_%s", Metrics.LAST_WRITE_LOCATION_PREFIX, indexOfSubTask);
        String lastWriteNum =
                String.format("%s_%s", Metrics.LAST_WRITE_NUM__PREFIX, indexOfSubTask);

        accumulatorCollector =
                new AccumulatorCollector(
                        context,
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

    /** 初始化对象大小计算器 */
    private void initRowSizeCalculator() {
        rowSizeCalculator =
                RowSizeCalculator.getRowSizeCalculator(
                        config.getRowSizeCalculatorType(), useAbstractColumn);
    }

    /** 初始化速率限制器 */
    private void initByteRateLimiter() {
        if (config.getSpeedBytes() > 0) {
            this.byteRateLimiter =
                    new ByteRateLimiter(accumulatorCollector, config.getSpeedBytes());
            this.byteRateLimiter.start();
        }
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
            formatState = new FormatState(indexOfSubTask, null);
        } else {
            numReadCounter.add(formatState.getMetricValue(Metrics.NUM_READS));
            bytesReadCounter.add(formatState.getMetricValue(Metrics.READ_BYTES));
            durationCounter.add(formatState.getMetricValue(Metrics.READ_DURATION));
        }
    }

    public FormatState getFormatState() {
        if (formatState != null && numReadCounter != null && inputMetric != null) {
            formatState.setMetric(inputMetric.getMetricCounters());
        }
        return formatState;
    }

    /** 使用自定义的指标输出器把增量指标打到自定义插件 */
    protected boolean useCustomReporter() {
        return false;
    }

    /** 为了保证增量数据的准确性，指标输出失败时使任务失败 */
    protected boolean makeTaskFailedWhenReportFailed() {
        return false;
    }

    protected abstract InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception;

    protected abstract void openInternal(InputSplit inputSplit) throws IOException;

    protected abstract RowData nextRecordInternal(RowData rowData) throws ReadRecordException;

    protected abstract void closeInternal() throws IOException;
}
