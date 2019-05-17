/**
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
import com.dtstack.flinkx.latch.Latch;
import com.dtstack.flinkx.latch.LocalLatch;
import com.dtstack.flinkx.latch.MetricLatch;
import com.dtstack.flinkx.metrics.OutputMetric;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.URLUtil;
import com.dtstack.flinkx.writer.DirtyDataManager;
import com.dtstack.flinkx.writer.ErrorLimiter;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.HttpClientBuilder;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static com.dtstack.flinkx.writer.WriteErrorTypes.*;

/**
 * Abstract Specification for all the OutputFormat defined in flinkx plugins
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class  RichOutputFormat extends org.apache.flink.api.common.io.RichOutputFormat<Row> implements CleanupWhenUnsuccessful {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    public static final String RUNNING_STATE = "RUNNING";

    /** Dirty data manager */
    protected DirtyDataManager dirtyDataManager;

    /** Dirty data storage path */
    protected String dirtyPath;

    /** The hadoop config for dirty data storage */
    protected Map<String,String> dirtyHadoopConfig;

    /** The source table field names  */
    protected List<String> srcFieldNames;

    /** 批量提交条数 */
    protected int batchInterval = 1;

    /** 存储用于批量写入的数据 */
    protected List<Row> rows = new ArrayList();

    /** 总记录数 */
    protected LongCounter numWriteCounter;

    /** 错误记录数 */
    protected IntCounter errCounter;

    /** Number of null pointer errors */
    protected IntCounter nullErrCounter;

    /** Number of primary key conflict errors */
    protected IntCounter duplicateErrCounter;

    /** Number of type conversion errors */
    protected IntCounter conversionErrCounter;

    /** Number of other errors */
    protected IntCounter otherErrCounter;

    /** 错误限制 */
    protected ErrorLimiter errorLimiter;

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

    protected boolean readyWrite = false;

    protected transient OutputMetric outputMetric;

    public DirtyDataManager getDirtyDataManager() {
        return dirtyDataManager;
    }

    public String getDirtyPath() {
        return dirtyPath;
    }

    public void setDirtyPath(String dirtyPath) {
        this.dirtyPath = dirtyPath;
    }

    public Map<String, String> getDirtyHadoopConfig() {
        return dirtyHadoopConfig;
    }

    public void setDirtyHadoopConfig(Map<String, String> dirtyHadoopConfig) {
        this.dirtyHadoopConfig = dirtyHadoopConfig;
    }

    public List<String> getSrcFieldNames() {
        return srcFieldNames;
    }

    public void setSrcFieldNames(List<String> srcFieldNames) {
        this.srcFieldNames = srcFieldNames;
    }

    @Override
    public void configure(Configuration parameters) {
        // do nothing
    }

    protected abstract void openInternal(int taskNumber, int numTasks) throws IOException;

    /**
     * The method that I don't know how to say
     * @param taskNumber The number of the parallel instance.
     * @param numTasks The number of parallel tasks.
     * @throws IOException
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        LOG.info("subtask[" + taskNumber +  " open start");
        this.taskNumber = taskNumber;
        context = (StreamingRuntimeContext) getRuntimeContext();
        this.numTasks = numTasks;

        //错误记录数
        errCounter = context.getIntCounter(Metrics.NUM_ERRORS);
        nullErrCounter = context.getIntCounter(Metrics.NUM_NULL_ERRORS);
        duplicateErrCounter = context.getIntCounter(Metrics.NUM_DUPLICATE_ERRORS);
        conversionErrCounter = context.getIntCounter(Metrics.NUM_CONVERSION_ERRORS);
        otherErrCounter = context.getIntCounter(Metrics.NUM_OTHER_ERRORS);

        //总记录数
        numWriteCounter = context.getLongCounter(Metrics.NUM_WRITES);

        outputMetric = new OutputMetric(context, errCounter, nullErrCounter, duplicateErrCounter, conversionErrCounter, otherErrCounter, numWriteCounter);

        Map<String, String> vars = context.getMetricGroup().getAllVariables();

        if(vars != null && vars.get(Metrics.JOB_NAME) != null) {
            jobName = vars.get(Metrics.JOB_NAME);
        }

        if(vars!= null && vars.get(Metrics.JOB_ID) != null) {
            jobId = vars.get(Metrics.JOB_ID);
        }

        //启动错误限制
        if(StringUtils.isNotBlank(monitorUrl)) {
            if(errors != null || errorRatio != null) {
                errorLimiter = new ErrorLimiter(context, monitorUrl, errors, errorRatio, 2);
                errorLimiter.start();
            }
        }

        //启动脏数据管理
        if(StringUtils.isNotBlank(dirtyPath)) {
            dirtyDataManager = new DirtyDataManager(dirtyPath, dirtyHadoopConfig, srcFieldNames.toArray(new String[srcFieldNames.size()]));
            dirtyDataManager.open();
        }

        if(needWaitBeforeOpenInternal()) {
            Latch latch = newLatch("#1");
            beforeOpenInternal();
            latch.addOne();
            latch.waitUntil(numTasks);
        }

        openInternal(taskNumber, numTasks);

        // Do something before starting writing records
        if(needWaitBeforeWriteRecords()) {
            Latch latch = newLatch("#2");
            beforeWriteRecords();
            latch.addOne();
            latch.waitUntil(numTasks);
        }

        if(restoreConfig == null){
            restoreConfig = RestoreConfig.defaultConfig();
        } else if(restoreConfig.isRestore()){
            if(formatState == null){
                formatState = new FormatState(taskNumber, null);
            } else {
                initState = formatState.getState();
                numWriteCounter.add(formatState.getNumberWrite());
            }
        }
    }

    protected boolean needWaitBeforeOpenInternal() {
        return false;
    }

    protected void beforeOpenInternal() {

    }

    protected void writeSingleRecord(Row row) {
        // 若错误数超过限制,则抛异常而退出
        if(errorLimiter != null) {
            errorLimiter.acquire();
        }

        try {
            writeSingleRecordInternal(row);

            if(!restoreConfig.isRestore()){
                // 总记录数加1
                numWriteCounter.add(1);
            }
        } catch(WriteRecordException e) {
            errCounter.add(1);
            String errMsg = e.getMessage();

            int pos = e.getColIndex();
            if (pos != -1) {
               errMsg += recordConvertDetailErrorMessage(pos, e.getRow());
            }

            if(errorLimiter != null) {
                errorLimiter.setErrMsg(errMsg);
                errorLimiter.setErrorData(row);
            }

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

            LOG.error(e.getMessage());
        }

    }

    protected String recordConvertDetailErrorMessage(int pos, Row row) {
        return getClass().getName() + " WriteRecord error: when converting field[" + pos + "] in Row(" + row + ")";
    }

    protected abstract void writeSingleRecordInternal(Row row) throws WriteRecordException;

    protected void writeMultipleRecords() throws Exception {
        writeMultipleRecordsInternal();

        if(!restoreConfig.isRestore()){
            numWriteCounter.add(rows.size());
        }
    }

    protected abstract void writeMultipleRecordsInternal() throws Exception;

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
        /*
         * Remove channel information from the data
         */
        Row internalRow = new Row(row.getArity() - 1);
        for (int i = 0; i < internalRow.getArity(); i++) {
            internalRow.setField(i, row.getField(i));
        }

        if(restoreConfig.isRestore() && !readyWrite && initState != null){
            Object currentState = internalRow.getField(restoreConfig.getRestoreColumnIndex());
            if(currentState != null){
                readyWrite = currentState.toString().compareTo(initState.toString()) > 0;
            }

            if(!readyWrite){
                return;
            }
        }

        if(batchInterval <= 1) {
            writeSingleRecord(internalRow);
        } else {
            rows.add(internalRow);
            if(rows.size() == batchInterval) {
                writeRecordInternal();
            }
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("subtask[" + taskNumber + "] close()");

        try{
            if(rows.size() != 0) {
                writeRecordInternal();
            }
            if(needWaitBeforeCloseInternal()) {
                Latch latch = newLatch("#3");
                beforeCloseInternal();
                latch.addOne();
                latch.waitUntil(numTasks);
            }
        }finally {
            try{
                closeInternal();
                if(needWaitAfterCloseInternal()) {
                    Latch latch = newLatch("#4");
                    latch.addOne();
                    latch.waitUntil(numTasks);
                }
                afterCloseInternal();
            }finally {
                if(dirtyDataManager != null) {
                    dirtyDataManager.close();
                }

                if(errorLimiter != null) {
                    try{
                        // Wait a while before checking dirty data
                        Latch latch = newLatch("#5");
                        latch.addOne();
                        latch.waitUntil(numTasks);

                        errorLimiter.updateErrorInfo();
                    } catch (Exception e){
                        LOG.warn("Update error info error when task closing:{}", e);
                    }

                    errorLimiter.acquire();
                    errorLimiter.stop();
                }
            }
            LOG.info("subtask[" + taskNumber + "] close() finished");
        }
    }

    public void closeInternal() throws IOException {

    }

    @Override
    public void tryCleanupOnError() throws Exception {

    }

    protected String getTaskState() throws IOException{
        if (StringUtils.isEmpty(monitorUrl)) {
            return null;
        }

        String taskState;
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        String monitors = String.format("%s/jobs/%s", monitorUrl, jobId);
        LOG.info("Monitor url:{}", monitors);

        JsonParser parser = new JsonParser();
        for (int i = 0; i < 5; i++) {
            try{
                String response = URLUtil.get(httpClient, monitors);
                LOG.info("response:{}", response);

                JsonObject obj = parser.parse(response).getAsJsonObject();
                taskState = obj.get("state").getAsString();
                LOG.info("Job state is:{}", taskState);

                if(taskState != null){
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

    protected Latch newLatch(String latchName) {
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
}
