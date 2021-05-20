/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.outputformat;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.factory.DTThreadFactory;
import com.dtstack.flinkx.metrics.AccumulatorCollector;
import com.dtstack.flinkx.metrics.BaseMetric;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.sink.DirtyDataManager;
import com.dtstack.flinkx.sink.ErrorLimiter;
import com.dtstack.flinkx.sink.WriteErrorTypes;
import com.dtstack.flinkx.util.ExceptionUtil;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Abstract Specification for all the OutputFormat defined in flinkx plugins
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public abstract class BaseRichOutputFormat extends RichOutputFormat<RowData> implements CleanupWhenUnsuccessful, InitializeOnMaster, FinalizeOnMaster {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    public static final int LOG_PRINT_INTERNAL = 2000;

    /** 环境上下文 */
    protected StreamingRuntimeContext context;
    /** 是否开启了checkpoint */
    protected boolean checkpointEnabled;

    /** 任务名称 */
    protected String jobName = "defaultJobName";
    /** 任务id */
    protected String jobId;
    /** 任务索引id */
    protected int taskNumber;
    /** 子任务数量 */
    protected int numTasks;
    /** 任务开始时间, openInputFormat()开始计算 */
    protected long startTime;

    protected String formatId;
    /** checkpoint状态缓存map */
    protected FormatState formatState;

    /**
     * 虽然开启cp，是否采用定时器和一定条数让下游数据可见。
     * EXACTLY_ONCE：否。
     * AT_LEAST_ONCE：只要数据条数或者超时即可见
     */
    protected String checkpointMode;
    /** 定时提交数据服务 */
    protected transient ScheduledExecutorService scheduler;
    /** 定时提交数据服务返回结果 */
    protected transient ScheduledFuture scheduledFuture;
    /** 定时提交数据服务间隔时间，单位毫秒 */
    protected long flushIntervalMills;
    /** 任务公共配置 */
    protected FlinkxCommonConf config;
    /** BaseRichOutputFormat是否结束 */
    protected transient volatile boolean closed = false;
    /** 批量提交条数 */
    protected int batchSize = 1;
    /** 最新读取的数据 */
    protected RowData lastRow = null;

    /** 存储用于批量写入的数据 */
    protected transient List<RowData> rows;
    /** 数据类型转换器 */
    protected AbstractRowConverter rowConverter;
    /** 是否需要初始化脏数据和累加器，目前只有hive插件该参数设置为false */
    protected boolean initAccumulatorAndDirty = true;
    /** 脏数据管理器 */
    protected DirtyDataManager dirtyDataManager;
    /** 脏数据限制器 */
    protected ErrorLimiter errorLimiter;
    /** 输出指标组 */
    protected transient BaseMetric outputMetric;

    /** 累加器收集器 */
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

    @Override
    public void initializeGlobal(int parallelism) {
        //任务开始前操作，在configure前调用。
    }

    @Override
    public void configure(Configuration parameters) {
        // do nothing
    }

    @Override
    public void finalizeGlobal(int parallelism) {
        //任务结束后操作。
    }

    /**
     * 打开资源的前后做一些初始化操作
     *
     * @param taskNumber 任务索引id
     * @param numTasks 子任务数量
     *
     * @throws IOException
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        LOG.info("subtask[{}] open start", taskNumber);
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
        this.context = (StreamingRuntimeContext) getRuntimeContext();
        this.checkpointEnabled = context.isCheckpointingEnabled();
        this.rows = new ArrayList<>(1024);

        ExecutionConfig executionConfig = context.getExecutionConfig();
        checkpointMode = executionConfig.getGlobalJobParameters().toMap()
                .getOrDefault("sql.checkpoint.mode", CheckpointingMode.EXACTLY_ONCE.toString());
        Map<String, String> vars = context.getMetricGroup().getAllVariables();
        if(vars != null){
            jobName = vars.getOrDefault(Metrics.JOB_NAME, "defaultJobName");
            jobId = vars.get(Metrics.JOB_NAME);
        }

        initStatisticsAccumulator();
        initRestoreInfo();
        initTimingSubmitTask();

        if (initAccumulatorAndDirty) {
            initAccumulatorCollector();
            initErrorLimiter();
            initDirtyDataManager();
        }

        openInternal(taskNumber, numTasks);
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void writeRecord(RowData rowData) {
        int size = 0;
        if (batchSize <= 1) {
            writeSingleRecord(rowData);
            size = 1;
        } else {
            synchronized (rows){
                rows.add(rowData);
                if (rows.size() == batchSize) {
                    writeRecordInternal();
                    size = batchSize;
                }
            }
        }

        updateDuration();
        numWriteCounter.add(size);
        bytesWriteCounter.add(ObjectSizeCalculator.getObjectSize(rowData));
        if(!checkpointEnabled){
            snapshotWriteCounter.add(size);
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("subtask[{}] close()", taskNumber);

        try {
            if (closed) {
                return;
            }
            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }
            // when exist data
            synchronized (rows){
                if (rows.size() != 0) {
                    writeRecordInternal();
                }
            }

            if (durationCounter != null) {
                updateDuration();
            }
            this.closed = true;
        } finally {
            try {
                closeInternal();
                if (outputMetric != null) {
                    outputMetric.waitForReportMetrics();
                }
            } finally {
                if (dirtyDataManager != null) {
                    dirtyDataManager.close();
                }

                if (errorLimiter != null) {
                    errorLimiter.updateErrorInfo();
                    errorLimiter.checkErrorLimit();
                }
                if (accumulatorCollector != null) {
                    accumulatorCollector.close();
                }
            }
            LOG.info("subtask[{}}] close() finished", taskNumber);
        }
    }

    @Override
    public void tryCleanupOnError() throws Exception {}

    /**
     * 初始化累加器指标
     */
    protected void initStatisticsAccumulator() {
        errCounter = context.getLongCounter(Metrics.NUM_ERRORS);
        nullErrCounter = context.getLongCounter(Metrics.NUM_NULL_ERRORS);
        duplicateErrCounter = context.getLongCounter(Metrics.NUM_DUPLICATE_ERRORS);
        conversionErrCounter = context.getLongCounter(Metrics.NUM_CONVERSION_ERRORS);
        otherErrCounter = context.getLongCounter(Metrics.NUM_OTHER_ERRORS);
        numWriteCounter = context.getLongCounter(Metrics.NUM_WRITES);
        snapshotWriteCounter = context.getLongCounter(Metrics.SNAPSHOT_WRITES);
        bytesWriteCounter = context.getLongCounter(Metrics.WRITE_BYTES);
        durationCounter = context.getLongCounter(Metrics.WRITE_DURATION);

        outputMetric = new BaseMetric(context);
        outputMetric.addMetric(Metrics.NUM_ERRORS, errCounter);
        outputMetric.addMetric(Metrics.NUM_NULL_ERRORS, nullErrCounter);
        outputMetric.addMetric(Metrics.NUM_DUPLICATE_ERRORS, duplicateErrCounter);
        outputMetric.addMetric(Metrics.NUM_CONVERSION_ERRORS, conversionErrCounter);
        outputMetric.addMetric(Metrics.NUM_OTHER_ERRORS, otherErrCounter);
        outputMetric.addMetric(Metrics.NUM_WRITES, numWriteCounter, true);
        outputMetric.addMetric(Metrics.SNAPSHOT_WRITES, snapshotWriteCounter);
        outputMetric.addMetric(Metrics.WRITE_BYTES, bytesWriteCounter, true);
        outputMetric.addMetric(Metrics.WRITE_DURATION, durationCounter);
    }

    /**
     * 初始化累加器收集器
     */
    private void initAccumulatorCollector() {
        accumulatorCollector = new AccumulatorCollector(context, Metrics.METRIC_SINK_LIST);
        accumulatorCollector.start();
    }

    /**
     * 初始化脏数据限制器
     */
    private void initErrorLimiter() {
        if (config.getErrorRecord() >= 0 || config.getErrorPercentage() > 0) {
            Double errorRatio = null;
            if (config.getErrorPercentage() > 0) {
                errorRatio = (double) config.getErrorPercentage();
            }
            errorLimiter = new ErrorLimiter(accumulatorCollector, config.getErrorRecord(), errorRatio);
            LOG.info("init dirtyDataManager: {}", this.errorLimiter);
        }
    }

    /**
     * 初始化脏数据管理器
     */
    private void initDirtyDataManager() {
        if (StringUtils.isNotBlank(config.getDirtyDataPath())) {
            dirtyDataManager = new DirtyDataManager(
                    config.getDirtyDataPath(),
                    config.getDirtyDataHadoopConf(),
                    config.getFieldNameList().toArray(new String[0]),
                    jobId);
            dirtyDataManager.open();
            LOG.info("init dirtyDataManager: {}", this.dirtyDataManager);
        }
    }

    /**
     * 从checkpoint状态缓存map中恢复上次任务的指标信息
     */
    private void initRestoreInfo() {
        if (formatState == null) {
            formatState = new FormatState(taskNumber, null);
        } else {
            errCounter.add(formatState.getMetricValue(Metrics.NUM_ERRORS));
            nullErrCounter.add(formatState.getMetricValue(Metrics.NUM_NULL_ERRORS));
            duplicateErrCounter.add(formatState.getMetricValue(Metrics.NUM_DUPLICATE_ERRORS));
            conversionErrCounter.add(formatState.getMetricValue(Metrics.NUM_CONVERSION_ERRORS));
            otherErrCounter.add(formatState.getMetricValue(Metrics.NUM_OTHER_ERRORS));

            //use snapshot write count
            numWriteCounter.add(formatState.getMetricValue(Metrics.SNAPSHOT_WRITES));

            snapshotWriteCounter.add(formatState.getMetricValue(Metrics.SNAPSHOT_WRITES));
            bytesWriteCounter.add(formatState.getMetricValue(Metrics.WRITE_BYTES));
            durationCounter.add(formatState.getMetricValue(Metrics.WRITE_DURATION));
        }
    }

    /**
     * 开启定时提交数据
     */
    private void initTimingSubmitTask() {
        if (batchSize > 1 && flushIntervalMills > 0) {
            this.scheduler = new ScheduledThreadPoolExecutor(1, new DTThreadFactory("timer-data-write-thread"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (rows) {
                    if (closed) {
                        return;
                    }
                    try {
                        if(!rows.isEmpty()){
                            writeRecordInternal();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Writing records failed.", e);
                    }
                }
            }, flushIntervalMills, flushIntervalMills, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 数据单条写出
     * @param rowData 单条数据
     */
    protected void writeSingleRecord(RowData rowData) {
        if (errorLimiter != null) {
            errorLimiter.checkErrorLimit();
        }

        try {
            writeSingleRecordInternal(rowData);
        } catch (WriteRecordException e) {
            updateDirtyDataMsg(rowData, e);
            if (LOG.isTraceEnabled()) {
                LOG.trace("write error rowData, rowData = {}, e = {}", rowData.toString(), ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /**
     * 数据批量写出
     */
    protected void writeRecordInternal() {
        try {
            writeMultipleRecordsInternal();
        } catch (Exception e) {
            //批量写异常转为单条写
            rows.forEach(this::writeSingleRecord);
        }
        rows.clear();
    }

    /**
     * 更新脏数据信息
     * @param rowData 当前读取的数据
     * @param e 异常
     */
    private void updateDirtyDataMsg(RowData rowData, WriteRecordException e) {
        errCounter.add(1);

        String errMsg = ExceptionUtil.getErrorMessage(e);
        int pos = e.getColIndex();
        if (pos != -1) {
            errMsg += recordConvertDetailErrorMessage(pos, e.getRowData());
        }

        //每2000条打印一次脏数据
        if (errCounter.getLocalValue() % LOG_PRINT_INTERNAL == 0) {
            LOG.error(errMsg);
        }

        if (errorLimiter != null) {
            errorLimiter.setErrMsg(errMsg);
            errorLimiter.setErrorData(rowData);
        }

        if (dirtyDataManager != null) {
            String errorType = dirtyDataManager.writeData(rowData, e);
            switch (errorType){
                case WriteErrorTypes.ERR_NULL_POINTER:
                    nullErrCounter.add(1);
                    break;
                case WriteErrorTypes.ERR_FORMAT_TRANSFORM:
                    conversionErrCounter.add(1);
                    break;
                case WriteErrorTypes.ERR_PRIMARY_CONFLICT:
                    duplicateErrCounter.add(1);
                    break;
                default:
                    otherErrCounter.add(1);
            }
        }
    }

    /**
     * 记录脏数据异常信息
     * @param pos 异常字段索引
     * @param rowData 当前读取的数据
     * @return 脏数据异常信息记录
     */
    protected String recordConvertDetailErrorMessage(int pos, RowData rowData) {
        return String.format("%s WriteRecord error: when converting field[%s] in Row(%s)", getClass().getName(), pos, rowData);
    }

    /**
     * 更新任务执行时间指标
     */
    private void updateDuration() {
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    /**
     * 更新checkpoint状态缓存map
     * @return
     */
    public FormatState getFormatState() throws Exception {
        if (formatState != null) {
            formatState.setMetric(outputMetric.getMetricCounters());
        }
        return formatState;
    }

    /**
     * 写出单条数据
     *
     * @param rowData 数据
     *
     * @throws WriteRecordException
     */
    protected abstract void writeSingleRecordInternal(RowData rowData) throws WriteRecordException;

    /**
     * 写出多条数据
     *
     * @throws Exception
     */
    protected abstract void writeMultipleRecordsInternal() throws Exception;

    /**
     * 子类实现，打开资源
     *
     * @param taskNumber 通道索引
     * @param numTasks 通道数量
     *
     * @throws IOException
     */
    protected abstract void openInternal(int taskNumber, int numTasks) throws IOException;

    /**
     * 子类实现，关闭资源
     *
     * @throws IOException
     */
    protected abstract void closeInternal() throws IOException;

    /**
     * checkpoint成功时操作
     *
     * @param checkpointId
     */
    public abstract void notifyCheckpointComplete(long checkpointId) throws Exception;

    /**
     * checkpoint失败时操作
     *
     * @param checkpointId
     */
    public abstract void notifyCheckpointAborted(long checkpointId) throws Exception;

    public void setRestoreState(FormatState formatState) {
        this.formatState = formatState;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getFormatId() {
        return formatId;
    }

    public void setFormatId(String formatId) {
        this.formatId = formatId;
    }

    public void setDirtyDataManager(DirtyDataManager dirtyDataManager) {
        this.dirtyDataManager = dirtyDataManager;
    }

    public void setErrorLimiter(ErrorLimiter errorLimiter) {
        this.errorLimiter = errorLimiter;
    }

    public FlinkxCommonConf getConfig() {
        return config;
    }

    public void setConfig(FlinkxCommonConf config) {
        this.config = config;
    }

    public void setFlushIntervalMills(long flushIntervalMills) {
        this.flushIntervalMills = flushIntervalMills;
    }

    public void setRowConverter(AbstractRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }
}
