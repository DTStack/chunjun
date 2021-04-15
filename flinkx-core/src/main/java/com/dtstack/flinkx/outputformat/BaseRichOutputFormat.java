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

import com.dtstack.flinkx.converter.AbstractRowConverter;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.factory.DTThreadFactory;
import com.dtstack.flinkx.log.DtLogger;
import com.dtstack.flinkx.metrics.AccumulatorCollector;
import com.dtstack.flinkx.metrics.BaseMetric;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.sink.DirtyDataManager;
import com.dtstack.flinkx.sink.ErrorLimiter;
import com.dtstack.flinkx.sink.WriteErrorTypes;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

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

    protected String formatId;

    public static final int LOG_PRINT_INTERNAL = 2000;

    protected FlinkxCommonConf config;

    /** Dirty data manager */
    protected DirtyDataManager dirtyDataManager;

    /** 批量提交条数 */
    protected int batchSize = 1;

    /** 存储用于批量写入的数据 */
    protected List<RowData> rows = new ArrayList<>();

    /** 总记录数 */
    protected LongCounter numWriteCounter;

    /** snapshot 中记录的总记录数 */
    protected LongCounter snapshotWriteCounter;

    /** 错误记录数 */
    protected LongCounter errCounter;

    /** Number of null pointer errors */
    protected LongCounter nullErrCounter;

    /** Number of primary key conflict errors */
    protected LongCounter duplicateErrCounter;

    /** Number of type conversion errors */
    protected LongCounter conversionErrCounter;

    /** Number of other errors */
    protected LongCounter otherErrCounter;

    /** 错误限制 */
    protected ErrorLimiter errorLimiter;

    protected LongCounter bytesWriteCounter;

    protected LongCounter durationCounter;

    /** 任务名 */
    protected String jobName = "defaultJobName";

    /** 子任务编号 */
    protected int taskNumber;

    /** 环境上下文 */
    protected StreamingRuntimeContext context;

    /** 是否开启了checkpoint */
    protected boolean checkpointEnabled;

    /** 虽然开启cp，是否采用定时器让下游数据可见。false：是，true：只要数据条数或者超时即可见 */
    protected boolean commitEnabled;

    /** 子任务数量 */
    protected int numTasks;

    protected String jobId;

    protected FormatState formatState;

    protected Object initState;

    protected transient BaseMetric outputMetric;

    protected AccumulatorCollector accumulatorCollector;

    private long startTime;

    protected boolean initAccumulatorAndDirty = true;

    private transient ScheduledExecutorService scheduler;

    private transient ScheduledFuture scheduledFuture;

    protected long flushIntervalMills;

    private transient volatile boolean closed = false;

    protected AbstractRowConverter rowConverter;

    @Override
    public void initializeGlobal(int parallelism) {
        //任务开始前操作，在configure前调用，overwrite by subclass
    }

    @Override
    public void configure(Configuration parameters) {
        // do nothing
    }

    @Override
    public void finalizeGlobal(int parallelism) {
        //任务结束后操作，overwrite by subclass
    }

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
     * 打开资源的前后做一些初始化操作
     *
     * @param taskNumber The number of the parallel instance.
     * @param numTasks The number of parallel tasks.
     *
     * @throws IOException
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        LOG.info("subtask[{}] open start", taskNumber);
        this.taskNumber = taskNumber;
        this.context = (StreamingRuntimeContext) getRuntimeContext();
        this.checkpointEnabled = context.isCheckpointingEnabled();

        ExecutionConfig executionConfig = context.getExecutionConfig();
        commitEnabled = Boolean.parseBoolean(executionConfig
                .getGlobalJobParameters()
                .toMap()
                .get("sql.checkpoint.commitEnabled"));

        this.numTasks = numTasks;

        initStatisticsAccumulator();
        initJobInfo();

        if (initAccumulatorAndDirty) {
            initAccumulatorCollector();
//            openErrorLimiter();
//            openDirtyDataManager();
        }

        initRestoreInfo();

//        if(needWaitBeforeOpenInternal()) {
//            beforeOpenInternal();
//            waitWhile("#1");
//        }

        openInternal(taskNumber, numTasks);
//        if(needWaitBeforeWriteRecords()) {
//            beforeWriteRecords();
//            waitWhile("#2");
//        }
        // 是否开启定时提交数据
        if (batchSize > 1 && flushIntervalMills > 0) {
            this.scheduler = new ScheduledThreadPoolExecutor(
                    1,
                    new DTThreadFactory("jdbc-upsert-output-format"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (BaseRichOutputFormat.this) {
                    if (closed) {
                        return;
                    }
                    try {
                        writeRecordInternal();
                    } catch (Exception e) {
                        throw new RuntimeException("Writing records to JDBC failed.", e);
                    }
                }
            }, flushIntervalMills, flushIntervalMills, TimeUnit.MILLISECONDS);
        }
    }

    private void initAccumulatorCollector() {
        accumulatorCollector = new AccumulatorCollector(context, Metrics.METRIC_LIST);
        accumulatorCollector.start();
    }

    protected void initRestoreInfo() {
        if (formatState == null) {
            formatState = new FormatState(taskNumber, null);
        } else {
            initState = formatState.getState();

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

    protected void initJobInfo() {
        Map<String, String> vars = context.getMetricGroup().getAllVariables();
        if (vars != null && vars.get(Metrics.JOB_NAME) != null) {
            jobName = vars.get(Metrics.JOB_NAME);
        }

        if (vars != null && vars.get(Metrics.JOB_ID) != null) {
            jobId = vars.get(Metrics.JOB_ID);
        }
    }

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

        startTime = System.currentTimeMillis();
    }

    private void openErrorLimiter() {
        if (config.getRecord() >= 0 || config.getPercentage() > 0) {
            Double errorRatio = null;
            if (config.getPercentage() > 0) {
                errorRatio = (double) config.getPercentage();
            }
            errorLimiter = new ErrorLimiter(accumulatorCollector, config.getRecord(), errorRatio);
        }
    }

    private void openDirtyDataManager() {
        if (StringUtils.isNotBlank(config.getDirtyDataPath())) {
            dirtyDataManager = new DirtyDataManager(
                    config.getDirtyDataPath(),
                    config.getDirtyDataHadoopConf(),
                    config.getFieldNameList().toArray(new String[0]),
                    jobId);
            dirtyDataManager.open();
            LOG.info("init dirtyDataManager, {}", this.dirtyDataManager);
        }
    }

    protected void writeSingleRecord(RowData rowData) {
        if (errorLimiter != null) {
            errorLimiter.acquire();
        }

        try {
            writeSingleRecordInternal(rowData);

            numWriteCounter.add(1);
//            snapshotWriteCounter.add(1);
        } catch (WriteRecordException e) {
            saveErrorData(rowData, e);
            updateStatisticsOfDirtyData(rowData, e);
            // 总记录数加1
            numWriteCounter.add(1);
            snapshotWriteCounter.add(1);

            if (dirtyDataManager == null && errCounter.getLocalValue() % LOG_PRINT_INTERNAL == 0) {
                LOG.error(e.getMessage());
            }
            if (DtLogger.isEnableTrace()) {
                LOG.trace(
                        "write error rowData, rowData = {}, e = {}",
                        rowData.toString(),
                        ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    private void saveErrorData(RowData rowData, WriteRecordException e) {
        errCounter.add(1);

        String errMsg = ExceptionUtil.getErrorMessage(e);
        int pos = e.getColIndex();
        if (pos != -1) {
            errMsg += recordConvertDetailErrorMessage(pos, e.getRowData());
        }

        if (errorLimiter != null) {
            errorLimiter.setErrMsg(errMsg);
            errorLimiter.setErrorData(rowData);
        }
    }

    private void updateStatisticsOfDirtyData(RowData rowData, WriteRecordException e) {
        if (dirtyDataManager != null) {
            String errorType = dirtyDataManager.writeData(rowData, e);
            if (WriteErrorTypes.ERR_NULL_POINTER.equals(errorType)) {
                nullErrCounter.add(1);
            } else if (WriteErrorTypes.ERR_FORMAT_TRANSFORM.equals(errorType)) {
                conversionErrCounter.add(1);
            } else if (WriteErrorTypes.ERR_PRIMARY_CONFLICT.equals(errorType)) {
                duplicateErrCounter.add(1);
            } else {
                otherErrCounter.add(1);
            }
        }
    }

    protected String recordConvertDetailErrorMessage(int pos, RowData rowData) {
        return getClass().getName() + " WriteRecord error: when converting field[" + pos
                + "] in Row(" + rowData + ")";
    }

    /**
     * 写出单条数据
     *
     * @param rowData 数据
     *
     * @throws WriteRecordException
     */
    protected abstract void writeSingleRecordInternal(RowData rowData) throws WriteRecordException;

    protected void writeMultipleRecords() throws Exception {
        writeMultipleRecordsInternal();
        if (numWriteCounter != null) {
            numWriteCounter.add(rows.size());
        }
    }

    /**
     * 写出多条数据
     *
     * @throws Exception
     */
    protected abstract void writeMultipleRecordsInternal() throws Exception;

    protected synchronized void writeRecordInternal() {
        try {
            writeMultipleRecords();
        } catch (Exception e) {
            rows.forEach(this::writeSingleRecord);
        }
        rows.clear();
    }

    @Override
    public void writeRecord(RowData rowData) {
//        Row internalRow = setChannelInfo(row);
        if (batchSize <= 1) {
            writeSingleRecord(rowData);
        } else {
            rows.add(rowData);
            if (rows.size() == batchSize) {
                writeRecordInternal();
            }
        }

        updateDuration();
        if (bytesWriteCounter != null) {
            bytesWriteCounter.add(rowData.toString().getBytes().length);
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("subtask[{}}] close()", taskNumber);

        try {
            // close scheduler
            if (closed) {
                return;
            }
            closed = true;
            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }
            // when exist data
            if (rows.size() != 0) {
                writeRecordInternal();
            }

            if (durationCounter != null) {
                updateDuration();
            }

//            if(needWaitBeforeCloseInternal()) {
//                beforeCloseInternal();
//                waitWhile("#3");
//            }
        } finally {
            try {
                closeInternal();
//                if(needWaitAfterCloseInternal()) {
//                    afterCloseInternal();
//                    waitWhile("#4");
//                }

                if (outputMetric != null) {
                    outputMetric.waitForReportMetrics();
                }
            } finally {
                if (dirtyDataManager != null) {
                    dirtyDataManager.close();
                }

                checkErrorLimit();
                if (accumulatorCollector != null) {
                    accumulatorCollector.close();
                }
            }
            LOG.info("subtask[{}}] close() finished", taskNumber);
        }
    }

    private void checkErrorLimit() {
        if (errorLimiter != null) {
            try {
                errorLimiter.updateErrorInfo();
            } catch (Exception e) {
                LOG.warn("Update error info error when task closing: ", e);
            }

            errorLimiter.acquire();
        }
    }

    private void updateDuration() {
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    public void closeInternal() throws IOException {
    }

    @Override
    public void tryCleanupOnError() throws Exception {
    }

    /**
     * Get the recover point of current channel
     *
     * @return DataRecoverPoint
     */
    public FormatState getFormatState() throws Exception {
        if (formatState != null) {
            formatState.setMetric(outputMetric.getMetricCounters());
        }
        return formatState;
    }

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

    /**
     * create LogicalType
     * @param fieldNames field Names
     * @param types field types
     * @return
     */
    protected LogicalType createRowType(List<String> fieldNames, List<String> types) {
        TableSchema.Builder builder = TableSchema.builder();
        for(int i = 0; i < types.size(); i++) {
            DataType dataType = convertToDataType(types.get(i));
            builder.add(TableColumn.physical(fieldNames.get(i), dataType));
        }

        return builder
                .build()
                .toRowDataType()
                .getLogicalType();
    }

    protected DataType convertToDataType(String s) {
        throw new RuntimeException("sub class must override");
    }

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

    public long getFlushIntervalMills() {
        return flushIntervalMills;
    }

    public void setFlushIntervalMills(long flushIntervalMills) {
        this.flushIntervalMills = flushIntervalMills;
    }

    public AbstractRowConverter getRowConverter() {
        return rowConverter;
    }

    public void setRowConverter(AbstractRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }
}
