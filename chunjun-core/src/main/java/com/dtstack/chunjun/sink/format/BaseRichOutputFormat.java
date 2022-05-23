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

package com.dtstack.chunjun.sink.format;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.DdlRowDataConvented;
import com.dtstack.chunjun.cdc.monitor.MonitorConf;
import com.dtstack.chunjun.cdc.monitor.fetch.DdlObserver;
import com.dtstack.chunjun.cdc.monitor.fetch.Event;
import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.dirty.DirtyConf;
import com.dtstack.chunjun.dirty.manager.DirtyManager;
import com.dtstack.chunjun.dirty.utils.DirtyConfUtil;
import com.dtstack.chunjun.enums.Semantic;
import com.dtstack.chunjun.factory.ChunJunThreadFactory;
import com.dtstack.chunjun.metrics.AccumulatorCollector;
import com.dtstack.chunjun.metrics.BaseMetric;
import com.dtstack.chunjun.metrics.RowSizeCalculator;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.sink.DirtyDataManager;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.NoRestartException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.DataSyncFactoryUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.event.EventCenter;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract Specification for all the OutputFormat defined in chunjun plugins
 *
 * <p>Company: www.dtstack.com
 *
 * <p>NOTE Four situations for checkpoint(cp): 1).Turn off cp, batch and timing directly submitted
 * to the database
 *
 * <p>2).Turn on cp and in AT_LEAST_ONCE model, batch and timing directly commit to the db .
 * snapshotState、notifyCheckpointComplete、notifyCheckpointAborted Does not interact with the db
 *
 * <p>3).Turn on cp and in EXACTLY_ONCE model, batch and timing pre commit to the db . snapshotState
 * pre commit、notifyCheckpointComplete real commit、notifyCheckpointAborted rollback
 *
 * <p>4).Turn on cp and in EXACTLY_ONCE model, when cp time out
 * snapshotState、notifyCheckpointComplete may never call, Only call notifyCheckpointAborted.this
 * maybe a problem ,should make users perceive
 *
 * @author huyifan.zju@163.com
 */
public abstract class BaseRichOutputFormat extends RichOutputFormat<RowData>
        implements CleanupWhenUnsuccessful, InitializeOnMaster, FinalizeOnMaster {

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

    /** 虽然开启cp，是否采用定时器和一定条数让下游数据可见。 EXACTLY_ONCE：否，遵循两阶段提交协议。 AT_LEAST_ONCE：是，只要数据条数或者到达定时时间即可见 */
    protected CheckpointingMode checkpointMode;
    /** 定时提交数据服务 */
    protected transient ScheduledExecutorService scheduler;
    /** 定时提交数据服务返回结果 */
    protected transient ScheduledFuture scheduledFuture;
    /** 定时提交数据服务间隔时间，单位毫秒 */
    protected long flushIntervalMills;
    /** 任务公共配置 */
    protected ChunJunCommonConf config;
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
    /** 输出指标组 */
    protected transient BaseMetric outputMetric;
    /** cp和flush互斥条件 */
    protected transient AtomicBoolean flushEnable;
    /** 当前事务的条数 */
    protected long rowsOfCurrentTransaction;

    /** A collection of field names filled in user scripts with constants removed */
    protected List<String> columnNameList = new ArrayList<>();
    /** A collection of field types filled in user scripts with constants removed */
    protected List<String> columnTypeList = new ArrayList<>();

    /** 累加器收集器 */
    protected AccumulatorCollector accumulatorCollector;
    /** 对象大小计算器 */
    protected RowSizeCalculator rowSizeCalculator;

    protected LongCounter bytesWriteCounter;
    protected LongCounter durationCounter;
    protected LongCounter numWriteCounter;
    protected LongCounter snapshotWriteCounter;
    protected LongCounter errCounter;
    protected LongCounter nullErrCounter;
    protected LongCounter duplicateErrCounter;
    protected LongCounter conversionErrCounter;
    protected LongCounter otherErrCounter;

    protected Semantic semantic;

    /** the manager of dirty data. */
    protected DirtyManager dirtyManager;

    protected boolean executeDdlAble;
    protected EventCenter eventCenter;
    protected MonitorConf monitorConf;

    private transient volatile Exception timerWriteException;

    @Override
    public void initializeGlobal(int parallelism) {
        // 任务开始前操作，在configure前调用。
    }

    @Override
    public void configure(Configuration parameters) {
        // do nothing
    }

    @Override
    public void finalizeGlobal(int parallelism) {
        // 任务结束后操作。
    }

    /**
     * 打开资源的前后做一些初始化操作
     *
     * @param taskNumber 任务索引id
     * @param numTasks 子任务数量
     * @throws IOException
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
        this.context = (StreamingRuntimeContext) getRuntimeContext();
        this.checkpointEnabled = context.isCheckpointingEnabled();
        this.batchSize = config.getBatchSize();
        this.rows = new ArrayList<>(batchSize);
        this.executeDdlAble = config.isExecuteDdlAble();
        if (executeDdlAble) {
            this.eventCenter = new EventCenter("ddl");
            DdlObserver ddlObserver =
                    new DdlObserver(DataSyncFactoryUtil.discoverFetchBase(monitorConf));
            eventCenter.register(ddlObserver);
        }
        this.flushIntervalMills = config.getFlushIntervalMills();
        this.flushEnable = new AtomicBoolean(true);
        this.semantic = Semantic.getByName(config.getSemantic());

        ExecutionConfig.GlobalJobParameters params =
                context.getExecutionConfig().getGlobalJobParameters();
        DirtyConf dc = DirtyConfUtil.parseFromMap(params.toMap());
        this.dirtyManager = new DirtyManager(dc, this.context);

        checkpointMode =
                context.getCheckpointMode() == null
                        ? CheckpointingMode.AT_LEAST_ONCE
                        : context.getCheckpointMode();

        Map<String, String> vars = context.getMetricGroup().getAllVariables();
        if (vars != null) {
            jobName = vars.getOrDefault(Metrics.JOB_NAME, "defaultJobName");
            jobId = vars.get(Metrics.JOB_ID);
        }

        initStatisticsAccumulator();
        initRestoreInfo();
        initTimingSubmitTask();
        initRowSizeCalculator();

        if (initAccumulatorAndDirty) {
            initAccumulatorCollector();
        }
        openInternal(taskNumber, numTasks);
        this.startTime = System.currentTimeMillis();

        LOG.info(
                "[{}] open successfully, \ncheckpointMode = {}, \ncheckpointEnabled = {}, \nflushIntervalMills = {}, \nbatchSize = {}, \n[{}]: \n{} ",
                this.getClass().getSimpleName(),
                checkpointMode,
                checkpointEnabled,
                flushIntervalMills,
                batchSize,
                config.getClass().getSimpleName(),
                JsonUtil.toPrintJson(config));
    }

    @Override
    public synchronized void writeRecord(RowData rowData) {
        checkTimerWriteException();
        int size = 0;
        if (rowData instanceof DdlRowData) {
            executeDdlRowDataTemplate((DdlRowData) rowData);
            size = 1;
        } else {
            if (batchSize <= 1) {
                writeSingleRecord(rowData, numWriteCounter);
                size = 1;
            } else {
                rows.add(rowData);
                if (rows.size() >= batchSize) {
                    writeRecordInternal();
                    size = batchSize;
                }
            }
        }
        updateDuration();
        bytesWriteCounter.add(rowSizeCalculator.getObjectSize(rowData));
        if (checkpointEnabled) {
            snapshotWriteCounter.add(size);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        LOG.info("taskNumber[{}] close()", taskNumber);

        if (closed) {
            return;
        }

        if (Objects.isNull(rows)) {
            return;
        }

        Exception closeException = null;

        if (null != timerWriteException) {
            closeException = timerWriteException;
        }

        // when exist data
        int size = rows.size();
        if (size != 0) {
            try {
                writeRecordInternal();
                numWriteCounter.add(size);
            } catch (Exception e) {
                closeException = e;
            }
        }

        if (this.scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }

        try {
            closeInternal();
        } catch (Exception e) {
            LOG.warn("closeInternal() Exception:{}", ExceptionUtil.getErrorMessage(e));
        }

        updateDuration();

        if (outputMetric != null) {
            outputMetric.waitForReportMetrics();
        }

        if (dirtyDataManager != null) {
            try {
                dirtyDataManager.close();
            } catch (Exception e) {
                LOG.error(
                        "dirtyDataManager.close() Exception:{}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (accumulatorCollector != null) {
            accumulatorCollector.close();
        }

        if (dirtyManager != null) {
            dirtyManager.close();
        }

        if (closeException != null) {
            throw new RuntimeException(closeException);
        }

        LOG.info("subtask[{}}] close() finished", taskNumber);
        this.closed = true;
    }

    @Override
    public void tryCleanupOnError() throws Exception {}

    /** 初始化累加器指标 */
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
        outputMetric.addDirtyMetric(
                Metrics.DIRTY_DATA_COUNT, this.dirtyManager.getConsumedMetric());
        outputMetric.addDirtyMetric(
                Metrics.DIRTY_DATA_COLLECT_FAILED_COUNT,
                this.dirtyManager.getFailedConsumedMetric());
    }

    /** 初始化累加器收集器 */
    private void initAccumulatorCollector() {
        accumulatorCollector = new AccumulatorCollector(context, Metrics.METRIC_SINK_LIST);
        accumulatorCollector.start();
    }

    /** 初始化对象大小计算器 */
    protected void initRowSizeCalculator() {
        rowSizeCalculator =
                RowSizeCalculator.getRowSizeCalculator(config.getRowSizeCalculatorType());
    }

    /** 从checkpoint状态缓存map中恢复上次任务的指标信息 */
    private void initRestoreInfo() {
        if (formatState == null) {
            formatState = new FormatState(taskNumber, null);
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

    /** Turn on timed submission,Each result table is opened separately */
    private void initTimingSubmitTask() {
        if (batchSize > 1 && flushIntervalMills > 0) {
            LOG.info(
                    "initTimingSubmitTask() ,initialDelay:{}, delay:{}, MILLISECONDS",
                    flushIntervalMills,
                    flushIntervalMills);
            this.scheduler =
                    new ScheduledThreadPoolExecutor(
                            1, new ChunJunThreadFactory("timer-data-write-thread"));
            this.scheduledFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (BaseRichOutputFormat.this) {
                                    if (closed) {
                                        return;
                                    }
                                    try {
                                        if (!rows.isEmpty()) {
                                            writeRecordInternal();
                                        }
                                    } catch (Exception e) {
                                        LOG.error(
                                                "Writing records failed. {}",
                                                ExceptionUtil.getErrorMessage(e));
                                        timerWriteException = e;
                                    }
                                }
                            },
                            flushIntervalMills,
                            flushIntervalMills,
                            TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 数据单条写出
     *
     * @param rowData 单条数据
     */
    protected void writeSingleRecord(RowData rowData, LongCounter numWriteCounter) {
        try {
            writeSingleRecordInternal(rowData);
            numWriteCounter.add(1L);
        } catch (WriteRecordException e) {
            dirtyManager.collect(e.getRowData(), e, null);
            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "write error rowData, rowData = {}, e = {}",
                        rowData.toString(),
                        ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /** 数据批量写出 */
    protected synchronized void writeRecordInternal() {
        if (flushEnable.get()) {
            try {
                writeMultipleRecordsInternal();
                numWriteCounter.add(rows.size());
            } catch (Exception e) {
                // 批量写异常转为单条写
                rows.forEach(item -> writeSingleRecord(item, numWriteCounter));
            } finally {
                // Data is either recorded dirty data or written normally
                rows.clear();
            }
        }
    }

    private void checkTimerWriteException() {
        if (null != timerWriteException) {
            if (timerWriteException instanceof NoRestartException) {
                throw (NoRestartException) timerWriteException;
            } else if (timerWriteException instanceof RuntimeException) {
                throw (RuntimeException) timerWriteException;
            } else {
                throw new ChunJunRuntimeException("Writing records failed.", timerWriteException);
            }
        }
    }

    /**
     * 记录脏数据异常信息
     *
     * @param pos 异常字段索引
     * @param rowData 当前读取的数据
     * @return 脏数据异常信息记录
     */
    protected String recordConvertDetailErrorMessage(int pos, Object rowData) {
        return String.format(
                "%s WriteRecord error: when converting field[%s] in Row(%s)",
                getClass().getName(), pos, rowData);
    }

    /** 更新任务执行时间指标 */
    protected void updateDuration() {
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    /**
     * 更新checkpoint状态缓存map
     *
     * @return
     */
    public synchronized FormatState getFormatState() throws Exception {
        // not EXACTLY_ONCE model,Does not interact with the db
        if (Semantic.EXACTLY_ONCE == semantic) {
            try {
                LOG.info(
                        "getFormatState:Start preCommit, rowsOfCurrentTransaction: {}",
                        rowsOfCurrentTransaction);
                preCommit();
            } catch (Exception e) {
                LOG.error("preCommit error, e = {}", ExceptionUtil.getErrorMessage(e));
            } finally {
                flushEnable.compareAndSet(true, false);
            }
        } else {
            writeRecordInternal();
        }
        // set metric after preCommit
        formatState.setNumberWrite(numWriteCounter.getLocalValue());
        formatState.setMetric(outputMetric.getMetricCounters());
        LOG.info("format state:{}", formatState.getState());
        return formatState;
    }

    private void executeDdlRowDataTemplate(DdlRowData ddlRowData) {
        try {
            preExecuteDdlRwoData(ddlRowData);
            if (executeDdlAble) {
                executeDdlRwoData(ddlRowData);
                postExecuteDdlRwoData(ddlRowData);
            }
        } catch (Exception e) {
            LOG.error("execute ddl {} error", ddlRowData);
            throw new RuntimeException(e);
        }
    }

    /**
     * pre commit data
     *
     * @throws Exception
     */
    protected void preCommit() throws Exception {}

    /**
     * 写出单条数据
     *
     * @param rowData 数据
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
    public synchronized void notifyCheckpointComplete(long checkpointId) {
        if (Semantic.EXACTLY_ONCE == semantic) {
            try {
                commit(checkpointId);
                LOG.info("notifyCheckpointComplete:Commit success , checkpointId:{}", checkpointId);
            } catch (Exception e) {
                LOG.error("commit error, e = {}", ExceptionUtil.getErrorMessage(e));
            } finally {
                flushEnable.compareAndSet(false, true);
            }
        }
    }

    protected void preExecuteDdlRwoData(DdlRowData rowData) throws Exception {}

    protected void executeDdlRwoData(DdlRowData ddlRowData) throws Exception {
        throw new UnsupportedOperationException("not support execute ddlRowData");
    }

    protected void postExecuteDdlRwoData(DdlRowData ddlRowData) throws Exception {
        if (ddlRowData instanceof DdlRowDataConvented
                && !((DdlRowDataConvented) ddlRowData).conventSuccessful()) {
            return;
        }

        String tableIdentifier = ddlRowData.getTableIdentifier();
        String[] split = tableIdentifier.split("\\.");
        eventCenter.postMessage(
                new Event(split[0], split[1], ddlRowData.getLsn(), ddlRowData.getSql(), 2));
    }

    /**
     * commit data
     *
     * @param checkpointId
     * @throws Exception
     */
    public void commit(long checkpointId) throws Exception {}

    /**
     * checkpoint失败时操作
     *
     * @param checkpointId
     */
    public synchronized void notifyCheckpointAborted(long checkpointId) {
        if (Semantic.EXACTLY_ONCE == semantic) {
            try {
                rollback(checkpointId);
                LOG.info(
                        "notifyCheckpointAborted:rollback success , checkpointId:{}", checkpointId);
            } catch (Exception e) {
                LOG.error("rollback error, e = {}", ExceptionUtil.getErrorMessage(e));
            } finally {
                flushEnable.compareAndSet(false, true);
            }
        }
    }

    /**
     * rollback data
     *
     * @param checkpointId
     * @throws Exception
     */
    public void rollback(long checkpointId) throws Exception {}

    public void setRestoreState(FormatState formatState) {
        this.formatState = formatState;
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

    public ChunJunCommonConf getConfig() {
        return config;
    }

    public void setConfig(ChunJunCommonConf config) {
        this.config = config;
    }

    public void setRowConverter(AbstractRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }

    public void setDirtyManager(DirtyManager dirtyManager) {
        this.dirtyManager = dirtyManager;
    }

    public void setExecuteDdlAble(boolean executeDdlAble) {
        this.executeDdlAble = executeDdlAble;
    }

    public void setMonitorConf(MonitorConf monitorConf) {
        this.monitorConf = monitorConf;
    }
}
