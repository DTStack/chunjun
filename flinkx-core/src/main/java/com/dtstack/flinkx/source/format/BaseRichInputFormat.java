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

package com.dtstack.flinkx.source.format;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.dirty.DirtyConf;
import com.dtstack.flinkx.dirty.manager.DirtyManager;
import com.dtstack.flinkx.dirty.utils.DirtyConfUtil;
import com.dtstack.flinkx.metrics.AccumulatorCollector;
import com.dtstack.flinkx.metrics.BaseMetric;
import com.dtstack.flinkx.metrics.CustomReporter;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.source.ByteRateLimiter;
import com.dtstack.flinkx.throwable.ReadRecordException;
import com.dtstack.flinkx.util.DataSyncFactoryUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.JsonUtil;

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

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FlinkX里面所有自定义inputFormat的抽象基类
 *
 * <p>扩展了org.apache.flink.api.common.io.RichInputFormat, 因而可以通过{@link
 * #getRuntimeContext()}获取运行时执行上下文 自动完成 用户只需覆盖openInternal,closeInternal等方法, 无需操心细节
 *
 * @author jiangbo
 */
public abstract class BaseRichInputFormat extends RichInputFormat<RowData, InputSplit> {
    protected static final long serialVersionUID = 1L;

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    /** BaseRichInputFormat是否结束 */
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
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
    protected FlinkxCommonConf config;
    /** 数据类型转换器 */
    protected AbstractRowConverter rowConverter;
    /** 输入指标组 */
    protected transient BaseMetric inputMetric;
    /** 自定义的prometheus reporter，用于提交startLocation和endLocation指标 */
    protected transient CustomReporter customReporter;
    /** 累加器收集器 */
    protected AccumulatorCollector accumulatorCollector;
    /** checkpoint状态缓存map */
    protected FormatState formatState;

    protected LongCounter numReadCounter;
    protected LongCounter bytesReadCounter;
    protected LongCounter durationCounter;
    protected ByteRateLimiter byteRateLimiter;
    /** A collection of field names filled in user scripts with constants removed */
    protected List<String> columnNameList = new ArrayList<>();
    /** A collection of field types filled in user scripts with constants removed */
    protected List<String> columnTypeList = new ArrayList<>();
    /** dirty manager which collects the dirty data. */
    protected DirtyManager dirtyManager;
    /** BaseRichInputFormat是否已经初始化 */
    private boolean initialized = false;

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
            LOG.warn("error to create InputSplits", e);
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
        DirtyConf dc = DirtyConfUtil.parseFromMap(params.toMap());
        this.dirtyManager = new DirtyManager(dc, this.context);

        if (inputSplit instanceof ErrorInputSplit) {
            throw new RuntimeException(((ErrorInputSplit) inputSplit).getErrorMessage());
        }

        if (!initialized) {
            initAccumulatorCollector();
            initStatisticsAccumulator();
            initByteRateLimiter();
            initRestoreInfo();
            initialized = true;
        }

        openInternal(inputSplit);

        LOG.info(
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
    public RowData nextRecord(RowData rowData) {
        if (byteRateLimiter != null) {
            byteRateLimiter.acquire();
        }
        RowData internalRow = null;
        try {
            internalRow = nextRecordInternal(rowData);
        } catch (ReadRecordException e) {
            dirtyManager.collect(e.getRowData(), e, null);
        }
        if (internalRow != null) {
            updateDuration();
            if (numReadCounter != null) {
                numReadCounter.add(1);
            }
            if (bytesReadCounter != null) {
                bytesReadCounter.add(ObjectSizeCalculator.getObjectSize(internalRow));
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
        LOG.info("subtask input close finished");
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

    /** 使用自定义的指标输出器把增量指标打到自定义插件 */
    protected boolean useCustomReporter() {
        return false;
    }

    /** 为了保证增量数据的准确性，指标输出失败时使任务失败 */
    protected boolean makeTaskFailedWhenReportFailed() {
        return false;
    }

    /**
     * 由子类实现，创建数据分片
     *
     * @param minNumSplits 分片数量
     * @return 分片数组
     * @throws Exception 可能会出现连接数据源异常
     */
    protected abstract InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception;

    /**
     * 由子类实现，打开数据连接
     *
     * @param inputSplit 分片
     * @throws IOException 连接异常
     */
    protected abstract void openInternal(InputSplit inputSplit) throws IOException;

    /**
     * 由子类实现，读取一条数据
     *
     * @param rowData 需要创建和填充的数据
     * @return 读取的数据
     * @throws ReadRecordException 读取异常
     */
    protected abstract RowData nextRecordInternal(RowData rowData) throws ReadRecordException;

    /**
     * 由子类实现，关闭资源
     *
     * @throws IOException 连接关闭异常
     */
    protected abstract void closeInternal() throws IOException;

    public void setRestoreState(FormatState formatState) {
        this.formatState = formatState;
    }

    public FlinkxCommonConf getConfig() {
        return config;
    }

    public void setConfig(FlinkxCommonConf config) {
        this.config = config;
    }

    public void setRowConverter(AbstractRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }

    public void setDirtyManager(DirtyManager dirtyManager) {
        this.dirtyManager = dirtyManager;
    }
}
