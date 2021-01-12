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

import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.latch.BaseLatch;
import com.dtstack.flinkx.latch.LocalLatch;
import com.dtstack.flinkx.latch.MetricLatch;
import com.dtstack.flinkx.log.DtLogger;
import com.dtstack.flinkx.metrics.AccumulatorCollector;
import com.dtstack.flinkx.metrics.BaseMetric;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.UrlUtil;
import com.dtstack.flinkx.writer.DirtyDataManager;
import com.dtstack.flinkx.writer.ErrorLimiter;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.dtstack.flinkx.writer.WriteErrorTypes.ERR_FORMAT_TRANSFORM;
import static com.dtstack.flinkx.writer.WriteErrorTypes.ERR_NULL_POINTER;
import static com.dtstack.flinkx.writer.WriteErrorTypes.ERR_PRIMARY_CONFLICT;

/**
 * Abstract Specification for all the OutputFormat defined in flinkx plugins
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class BaseRichOutputFormat extends org.apache.flink.api.common.io.RichOutputFormat<Row> implements CleanupWhenUnsuccessful {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected String formatId;

    public static final String RUNNING_STATE = "RUNNING";

    public static final int LOG_PRINT_INTERNAL = 2000;

    /** Dirty data manager */
    protected DirtyDataManager dirtyDataManager;

    /** Dirty data storage path */
    protected String dirtyPath;

    /** The hadoop config for dirty data storage */
    protected Map<String,Object> dirtyHadoopConfig;

    /** The source table field names  */
    protected List<String> srcFieldNames;

    /** 批量提交条数 */
    protected int batchInterval = 1;

    /** 存储用于批量写入的数据 */
    protected List<Row> rows = new ArrayList();

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

    /** 错误阈值 */
    protected Integer errors;

    /** 错误比例阈值 */
    protected Double errorRatio;

    /** 任务名 */
    protected String jobName = "defaultJobName";

    /** 监控api根路径 */
    protected String monitorUrl;

    /** 子任务编号 */
    protected int taskNumber;

    /** 环境上下文 */
    protected StreamingRuntimeContext context;

    /** 子任务数量 */
    protected int numTasks;

    protected String jobId;

    protected RestoreConfig restoreConfig;

    protected FormatState formatState;

    protected Object initState;

    protected transient BaseMetric outputMetric;

    protected AccumulatorCollector accumulatorCollector;

    private long startTime;

    protected boolean initAccumulatorAndDirty = true;

    public String getDirtyPath() {
        return dirtyPath;
    }

    public void setDirtyPath(String dirtyPath) {
        this.dirtyPath = dirtyPath;
    }

    public Map<String, Object> getDirtyHadoopConfig() {
        return dirtyHadoopConfig;
    }

    public void setDirtyHadoopConfig(Map<String, Object> dirtyHadoopConfig) {
        this.dirtyHadoopConfig = dirtyHadoopConfig;
    }

    public void setDirtyDataManager(DirtyDataManager dirtyDataManager) {
        this.dirtyDataManager = dirtyDataManager;
    }

    public void setErrorLimiter(ErrorLimiter errorLimiter) {
        this.errorLimiter = errorLimiter;
    }

    public void setSrcFieldNames(List<String> srcFieldNames) {
        this.srcFieldNames = srcFieldNames;
    }

    @Override
    public void configure(Configuration parameters) {
        // do nothing
    }

    /**
     * 子类实现，打开资源
     *
     * @param taskNumber 通道索引
     * @param numTasks 通道数量
     * @throws IOException
     */
    protected abstract void openInternal(int taskNumber, int numTasks) throws IOException;

    /**
     * 打开资源的前后做一些初始化操作
     *
     * @param taskNumber The number of the parallel instance.
     * @param numTasks The number of parallel tasks.
     * @throws IOException
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        LOG.info("subtask[{}] open start", taskNumber);
        this.taskNumber = taskNumber;
        context = (StreamingRuntimeContext) getRuntimeContext();
        this.numTasks = numTasks;

        initStatisticsAccumulator();
        initJobInfo();

        if (initAccumulatorAndDirty) {
            initAccumulatorCollector();
            openErrorLimiter();
            openDirtyDataManager();
        }

        initRestoreInfo();

        if(needWaitBeforeOpenInternal()) {
            beforeOpenInternal();
            waitWhile("#1");
        }


        openInternal(taskNumber, numTasks);
        if(needWaitBeforeWriteRecords()) {
            beforeWriteRecords();
            waitWhile("#2");
        }
    }

    private void initAccumulatorCollector(){
        accumulatorCollector = new AccumulatorCollector(jobId, monitorUrl, getRuntimeContext(), 2,
                Arrays.asList(Metrics.NUM_ERRORS,
                        Metrics.NUM_NULL_ERRORS,
                        Metrics.NUM_DUPLICATE_ERRORS,
                        Metrics.NUM_CONVERSION_ERRORS,
                        Metrics.NUM_OTHER_ERRORS,
                        Metrics.NUM_WRITES,
                        Metrics.WRITE_BYTES,
                        Metrics.NUM_READS,
                        Metrics.WRITE_DURATION));
        accumulatorCollector.start();
    }

    protected void initRestoreInfo(){
        if(restoreConfig == null){
            restoreConfig = RestoreConfig.defaultConfig();
        } else if(restoreConfig.isRestore()){
            if(formatState == null){
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
    }

    protected void initJobInfo(){
        Map<String, String> vars = context.getMetricGroup().getAllVariables();
        if(vars != null && vars.get(Metrics.JOB_NAME) != null) {
            jobName = vars.get(Metrics.JOB_NAME);
        }

        if(vars!= null && vars.get(Metrics.JOB_ID) != null) {
            jobId = vars.get(Metrics.JOB_ID);
        }
    }

    protected void initStatisticsAccumulator(){
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

    private void openErrorLimiter(){
        if(errors != null || errorRatio != null) {
            errorLimiter = new ErrorLimiter(accumulatorCollector, errors, errorRatio);
        }
    }

    private void openDirtyDataManager(){
        if(StringUtils.isNotBlank(dirtyPath)) {
            dirtyDataManager = new DirtyDataManager(dirtyPath, dirtyHadoopConfig, srcFieldNames.toArray(new String[srcFieldNames.size()]), jobId);
            dirtyDataManager.open();
            LOG.info("init dirtyDataManager, {}", this.dirtyDataManager);
        }
    }

    protected boolean needWaitBeforeOpenInternal() {
        return false;
    }

    protected void beforeOpenInternal() {

    }

    protected void writeSingleRecord(Row row) {
        if(errorLimiter != null) {
            errorLimiter.acquire();
        }

        try {
            writeSingleRecordInternal(row);

            if(!restoreConfig.isRestore() || isStreamButNoWriteCheckpoint()){
                numWriteCounter.add(1);
                snapshotWriteCounter.add(1);
            }
        } catch(WriteRecordException e) {
            saveErrorData(row, e);
            updateStatisticsOfDirtyData(row, e);
            // 总记录数加1
            numWriteCounter.add(1);
            snapshotWriteCounter.add(1);

            if(dirtyDataManager == null && errCounter.getLocalValue() % LOG_PRINT_INTERNAL == 0){
                LOG.error(e.getMessage());
            }
            if(DtLogger.isEnableTrace()){
                LOG.trace("write error row, row = {}, e = {}", row.toString(), ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    protected boolean isStreamButNoWriteCheckpoint(){
        return false;
    }

    private void saveErrorData(Row row, WriteRecordException e){
        errCounter.add(1);

        String errMsg = ExceptionUtil.getErrorMessage(e);
        int pos = e.getColIndex();
        if (pos != -1) {
            errMsg += recordConvertDetailErrorMessage(pos, e.getRow());
        }

        if(errorLimiter != null) {
            errorLimiter.setErrMsg(errMsg);
            errorLimiter.setErrorData(row);
        }
    }

    private void updateStatisticsOfDirtyData(Row row, WriteRecordException e){
        if(dirtyDataManager != null) {
            String errorType = dirtyDataManager.writeData(row, e);
            if (ERR_NULL_POINTER.equals(errorType)){
                nullErrCounter.add(1);
            } else if(ERR_FORMAT_TRANSFORM.equals(errorType)){
                conversionErrCounter.add(1);
            } else if(ERR_PRIMARY_CONFLICT.equals(errorType)){
                duplicateErrCounter.add(1);
            } else {
                otherErrCounter.add(1);
            }
        }
    }

    protected String recordConvertDetailErrorMessage(int pos, Row row) {
        return getClass().getName() + " WriteRecord error: when converting field[" + pos + "] in Row(" + row + ")";
    }

    /**
     * 写出单条数据
     *
     * @param row 数据
     * @throws WriteRecordException
     */
    protected abstract void writeSingleRecordInternal(Row row) throws WriteRecordException;

    protected void writeMultipleRecords() throws Exception {
        writeMultipleRecordsInternal();
        if(!restoreConfig.isRestore()){
          if(numWriteCounter != null){
            numWriteCounter.add(rows.size());
          }
        }
    }

    /**
     * 写出多条数据
     *
     * @throws Exception
     */
    protected abstract void writeMultipleRecordsInternal() throws Exception;

    protected void notSupportBatchWrite(String writerName) {
        throw new UnsupportedOperationException(writerName + "不支持批量写入");
    }

    protected void writeRecordInternal() {
        try {
            writeMultipleRecords();
        } catch(Exception e) {
            if(restoreConfig.isRestore()){
                throw new RuntimeException(e);
            } else {
                rows.forEach(this::writeSingleRecord);
            }
        }
        rows.clear();
    }

    @Override
    public void writeRecord(Row row) throws IOException {
        Row internalRow = setChannelInfo(row);
        if(batchInterval <= 1) {
            writeSingleRecord(internalRow);
        } else {
            rows.add(internalRow);
            if(rows.size() == batchInterval) {
                writeRecordInternal();
            }
        }

        updateDuration();
        if(bytesWriteCounter!=null){
            bytesWriteCounter.add(row.toString().getBytes().length);
        }
    }

    private Row setChannelInfo(Row row){
        Row internalRow = new Row(row.getArity() - 1);
        for (int i = 0; i < internalRow.getArity(); i++) {
            internalRow.setField(i, row.getField(i));
        }
        return internalRow;
    }

    @Override
    public void close() throws IOException {
        LOG.info("subtask[{}}] close()", taskNumber);

        try{
            if(rows.size() != 0) {
                writeRecordInternal();
            }

            if(durationCounter != null){
                updateDuration();
            }

            if(needWaitBeforeCloseInternal()) {
                beforeCloseInternal();
                waitWhile("#3");
            }
        }finally {
            try{
                closeInternal();
                if(needWaitAfterCloseInternal()) {
                    afterCloseInternal();
                    waitWhile("#4");
                }

                if (outputMetric != null) {
                    outputMetric.waitForReportMetrics();
                }
            }finally {
                if(dirtyDataManager != null) {
                    dirtyDataManager.close();
                }

                checkErrorLimit();
                if(accumulatorCollector != null){
                    accumulatorCollector.close();
                }
            }
            LOG.info("subtask[{}}] close() finished", taskNumber);
        }
    }

    private void checkErrorLimit(){
        if(errorLimiter != null) {
            try{
                waitWhile("#5");

                errorLimiter.updateErrorInfo();
            } catch (Exception e){
                LOG.warn("Update error info error when task closing: ", e);
            }

            errorLimiter.acquire();
        }
    }

    private void updateDuration(){
        if(durationCounter!=null){
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    public void closeInternal() throws IOException {

    }

    @Override
    public void tryCleanupOnError() throws Exception {

    }

    protected String getTaskState() throws IOException{
        if (StringUtils.isEmpty(monitorUrl)) {
            return RUNNING_STATE;
        }

        String taskState = null;
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        String monitors = String.format("%s/jobs/overview", monitorUrl);
        LOG.info("Monitor url:{}", monitors);

        int retryNumber = 5;
        for (int i = 0; i < retryNumber; i++) {
            try{
                String response = UrlUtil.get(httpClient, monitors);
                LOG.info("response:{}", response);
                HashMap<String, ArrayList<HashMap<String, Object>>> map = GsonUtil.GSON.fromJson(response, new TypeToken<HashMap<String, ArrayList<HashMap<String, Object>>>>() {}.getType());
                List<HashMap<String, Object>> list = map.get("jobs");

                for (HashMap<String, Object> hashMap : list) {
                    String jid = (String)hashMap.get("jid");
                    if(Objects.equals(jid, jobId)){
                        taskState = (String) hashMap.get("state");
                        break;
                    }
                }
                LOG.info("Job state is:{}", taskState);

                if(taskState != null){
                    httpClient.close();
                    return taskState;
                }

                Thread.sleep(500);
            }catch (Exception e){
                LOG.info("Get job state error:{}", e.getMessage());
            }
        }

        httpClient.close();

        return RUNNING_STATE;
    }

    /**
     * Get the recover point of current channel
     * @return DataRecoverPoint
     */
    public FormatState getFormatState(){
        if (formatState != null){
            formatState.setMetric(outputMetric.getMetricCounters());
        }
        return formatState;
    }

    public void setRestoreState(FormatState formatState) {
        this.formatState = formatState;
    }

    protected boolean needWaitBeforeWriteRecords() {
        return false;
    }

    protected void beforeWriteRecords() {
        // nothing
    }

    protected boolean needWaitBeforeCloseInternal() {
        return false;
    }

    protected void beforeCloseInternal()  {
        // nothing
    }

    protected boolean needWaitAfterCloseInternal() {
        return false;
    }

    protected void afterCloseInternal()  {
        // nothing
    }

    protected void waitWhile(String latchName){
        BaseLatch latch = newLatch(latchName);
        latch.addOne();
        latch.waitUntil(numTasks);
    }

    protected BaseLatch newLatch(String latchName) {
        if(StringUtils.isNotBlank(monitorUrl)) {
            return new MetricLatch(getRuntimeContext(), monitorUrl, latchName);
        } else {
            return new LocalLatch(jobId + latchName);
        }
    }

    public int getBatchInterval() {
        return batchInterval;
    }

    public RestoreConfig getRestoreConfig() {
        return restoreConfig;
    }

    public String getFormatId() {
        return formatId;
    }

    public void setFormatId(String formatId) {
        this.formatId = formatId;
    }
}
