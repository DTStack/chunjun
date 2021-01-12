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

package com.dtstack.flinkx.inputformat;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.LogConfig;
import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.config.TestConfig;
import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.log.DtLogger;
import com.dtstack.flinkx.metrics.AccumulatorCollector;
import com.dtstack.flinkx.metrics.BaseMetric;
import com.dtstack.flinkx.metrics.CustomPrometheusReporter;
import com.dtstack.flinkx.reader.ByteRateLimiter;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.dtstack.flinkx.constants.ConfigConstant.KEY_CONFUSED_PASSWORD;
import static com.dtstack.flinkx.constants.ConfigConstant.KEY_CONTENT;
import static com.dtstack.flinkx.constants.ConfigConstant.KEY_PARAMETER;
import static com.dtstack.flinkx.constants.ConfigConstant.KEY_PASSWORD;
import static com.dtstack.flinkx.constants.ConfigConstant.KEY_READER;
import static com.dtstack.flinkx.constants.ConfigConstant.KEY_WRITER;

/**
 * FlinkX里面所有自定义inputFormat的抽象基类
 *
 * 扩展了org.apache.flink.api.common.io.RichInputFormat, 因而可以通过{@link #getRuntimeContext()}获取运行时执行上下文
 * 自动完成
 * 用户只需覆盖openInternal,closeInternal等方法, 无需操心细节
 *
 * @author jiangbo
 */
public abstract class BaseRichInputFormat extends org.apache.flink.api.common.io.RichInputFormat<Row, InputSplit> {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());
    protected String jobName = "defaultJobName";
    protected String jobId;
    protected LongCounter numReadCounter;
    protected LongCounter bytesReadCounter;
    protected LongCounter durationCounter;
    protected String monitorUrls;
    protected long bytes;
    protected ByteRateLimiter byteRateLimiter;

    protected RestoreConfig restoreConfig;
    protected LogConfig logConfig;
    protected DataTransferConfig dataTransferConfig;

    protected FormatState formatState;

    protected TestConfig testConfig = TestConfig.defaultConfig();

    protected transient BaseMetric inputMetric;

    protected int indexOfSubTask;

    protected long startTime;

    protected AccumulatorCollector accumulatorCollector;

    private boolean inited = false;

    private AtomicBoolean isClosed = new AtomicBoolean(false);

    protected transient CustomPrometheusReporter customPrometheusReporter;

    protected long numReadeForTest;

    /**
     * 有子类实现，打开数据连接
     *
     * @param inputSplit 分片
     * @throws IOException 连接异常
     */
    protected abstract void openInternal(InputSplit inputSplit) throws IOException;

    @Override
    public final void configure(Configuration parameters) {
        // do nothing
    }

    @Override
    public void openInputFormat() throws IOException {
        showConfig();
        initJobInfo();
        initPrometheusReporter();

        startTime = System.currentTimeMillis();
        DtLogger.config(logConfig, jobId);
    }

    @Override
    public final InputSplit[] createInputSplits(int i) throws IOException {
        try {
            return createInputSplitsInternal(i);
        } catch (Exception e){
            LOG.warn(ExceptionUtil.getErrorMessage(e));

            return createErrorInputSplit(e);
        }
    }

    private ErrorInputSplit[] createErrorInputSplit(Exception e){
        ErrorInputSplit[] inputSplits = new ErrorInputSplit[1];

        ErrorInputSplit errorInputSplit = new ErrorInputSplit(ExceptionUtil.getErrorMessage(e));
        inputSplits[0] = errorInputSplit;

        return inputSplits;
    }

    /**
     * 由子类实现，创建数据分片
     *
     * @param i 分片数量
     * @return 分片数组
     * @throws Exception 可能会出现连接数据源异常
     */
    protected abstract InputSplit[] createInputSplitsInternal(int i) throws Exception;

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        checkIfCreateSplitFailed(inputSplit);

        if(!inited){
            initAccumulatorCollector();
            initStatisticsAccumulator();
            openByteRateLimiter();
            initRestoreInfo();

            if(restoreConfig.isRestore()){
                formatState.setNumOfSubTask(indexOfSubTask);
            }

            inited = true;
        }

        openInternal(inputSplit);
    }

    @SuppressWarnings("unchecked")
    private void showConfig(){
        Map<String, Object> map = dataTransferConfig.getJob().getAll();
        List<Map<String,Object>> contentList = (List<Map<String,Object>>) map.get(KEY_CONTENT);
        for(Map<String,Object> contentMap : contentList) {
            //隐藏密码信息
            Map<String, Object> readerConfig = (Map<String, Object>)contentMap.get(KEY_READER);
            Map<String, Object> readerParameter = (Map<String, Object>)readerConfig.get(KEY_PARAMETER);
            if(readerParameter.containsKey(KEY_PASSWORD)){
                readerParameter.put(KEY_PASSWORD, KEY_CONFUSED_PASSWORD);
            }
            Map<String, Object> writerConfig = (Map<String, Object>)contentMap.get(KEY_WRITER);
            Map<String, Object> writerParameter = (Map<String, Object>)writerConfig.get(KEY_PARAMETER);
            if(writerParameter.containsKey(KEY_PASSWORD)){
                writerParameter.put(KEY_PASSWORD, KEY_CONFUSED_PASSWORD);
            }
        }
        LOG.info("configInfo : \n{}", GsonUtil.GSON.toJson(map));
    }

    private void checkIfCreateSplitFailed(InputSplit inputSplit){
        if (inputSplit instanceof ErrorInputSplit) {
            throw new RuntimeException(((ErrorInputSplit) inputSplit).getErrorMessage());
        }
    }

    private void initPrometheusReporter() {
        if (useCustomPrometheusReporter()) {
            customPrometheusReporter = new CustomPrometheusReporter(getRuntimeContext(), makeTaskFailedWhenReportFailed());
            customPrometheusReporter.open();
        }
    }

    protected boolean useCustomPrometheusReporter() {
        return false;
    }

    protected boolean makeTaskFailedWhenReportFailed(){
        return false;
    }

    private void initAccumulatorCollector(){
        String lastWriteLocation = String.format("%s_%s", Metrics.LAST_WRITE_LOCATION_PREFIX, indexOfSubTask);
        String lastWriteNum = String.format("%s_%s", Metrics.LAST_WRITE_NUM__PREFIX, indexOfSubTask);

        accumulatorCollector = new AccumulatorCollector(jobId, monitorUrls, getRuntimeContext(), 2,
                Arrays.asList(Metrics.NUM_READS,
                        Metrics.READ_BYTES,
                        Metrics.READ_DURATION,
                        Metrics.WRITE_BYTES,
                        Metrics.NUM_WRITES,
                        lastWriteLocation,
                        lastWriteNum));
        accumulatorCollector.start();
    }

    private void initJobInfo(){
        Map<String, String> vars = getRuntimeContext().getMetricGroup().getAllVariables();
        if(vars != null && vars.get(Metrics.JOB_NAME) != null) {
            jobName = vars.get(Metrics.JOB_NAME);
        }

        if(vars!= null && vars.get(Metrics.JOB_ID) != null) {
            jobId = vars.get(Metrics.JOB_ID);
        }

        if(vars != null && vars.get(Metrics.SUBTASK_INDEX) != null){
            indexOfSubTask = Integer.parseInt(vars.get(Metrics.SUBTASK_INDEX));
        }
    }

    private void openByteRateLimiter(){
        if (this.bytes > 0) {
            this.byteRateLimiter = new ByteRateLimiter(accumulatorCollector, this.bytes);
            this.byteRateLimiter.start();
        }
    }

    private void initStatisticsAccumulator(){
        numReadCounter = getRuntimeContext().getLongCounter(Metrics.NUM_READS);
        bytesReadCounter = getRuntimeContext().getLongCounter(Metrics.READ_BYTES);
        durationCounter = getRuntimeContext().getLongCounter(Metrics.READ_DURATION);

        inputMetric = new BaseMetric(getRuntimeContext());
        inputMetric.addMetric(Metrics.NUM_READS, numReadCounter, true);
        inputMetric.addMetric(Metrics.READ_BYTES, bytesReadCounter, true);
        inputMetric.addMetric(Metrics.READ_DURATION, durationCounter);
    }

    private void initRestoreInfo(){
        if(restoreConfig == null){
            restoreConfig = RestoreConfig.defaultConfig();
        } else if(restoreConfig.isRestore()){
            if(formatState == null){
                formatState = new FormatState(indexOfSubTask, null);
            } else {
                numReadCounter.add(formatState.getMetricValue(Metrics.NUM_READS));
                bytesReadCounter.add(formatState.getMetricValue(Metrics.READ_BYTES));
                durationCounter.add(formatState.getMetricValue(Metrics.READ_DURATION));
            }
        }
    }

    @Override
    public Row nextRecord(Row row) throws IOException {
        if(byteRateLimiter != null) {
            byteRateLimiter.acquire();
        }
        Row internalRow = nextRecordInternal(row);
        if(internalRow != null){
            internalRow = setChannelInformation(internalRow);

            updateDuration();
            if(numReadCounter !=null ){
                numReadCounter.add(1);
            }
            if(bytesReadCounter!=null){
                bytesReadCounter.add(internalRow.toString().getBytes().length);
            }
        }

        if (testConfig.errorTest() && testConfig.getFailedPerRecord() > 0) {
            numReadeForTest++;
            if (numReadeForTest > testConfig.getFailedPerRecord()) {
                throw new RuntimeException(testConfig.getErrorMsg());
            }
        }

        return internalRow;
    }

    private Row setChannelInformation(Row internalRow){
        Row rowWithChannel = new Row(internalRow.getArity() + 1);
        for (int i = 0; i < internalRow.getArity(); i++) {
            rowWithChannel.setField(i, internalRow.getField(i));
        }

        rowWithChannel.setField(internalRow.getArity(), indexOfSubTask);
        return rowWithChannel;
    }

    /**
     * Get the recover point of current channel
     * @return DataRecoverPoint
     */
    public FormatState getFormatState() {
        if (formatState != null && numReadCounter != null && inputMetric!= null) {
            formatState.setMetric(inputMetric.getMetricCounters());
        }
        return formatState;
    }

    /**
     * 由子类实现，读取一条数据
     *
     * @param row 需要创建和填充的数据
     * @return 读取的数据
     * @throws IOException 读取异常
     */
    protected abstract Row nextRecordInternal(Row row) throws IOException;

    @Override
    public void close() throws IOException {
        try{
            closeInternal();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeInputFormat() throws IOException {
        if (isClosed.get()) {
            return;
        }

        if(durationCounter != null){
            updateDuration();
        }

        if(byteRateLimiter != null){
            byteRateLimiter.stop();
        }

        if(accumulatorCollector != null){
            accumulatorCollector.close();
        }

        if (useCustomPrometheusReporter() && null != customPrometheusReporter) {
            customPrometheusReporter.report();
        }

        if(inputMetric != null){
            inputMetric.waitForReportMetrics();
        }

        if (useCustomPrometheusReporter() && null != customPrometheusReporter) {
            customPrometheusReporter.close();
        }

        isClosed.set(true);
        LOG.info("subtask input close finished");
    }

    private void updateDuration(){
        if(durationCounter !=null ){
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    /**
     * 由子类实现，关闭资源
     *
     * @throws IOException 连接关闭异常
     */
    protected abstract  void closeInternal() throws IOException;

    @Override
    public final BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public final InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    public void setRestoreState(FormatState formatState) {
        this.formatState = formatState;
    }

    public RestoreConfig getRestoreConfig() {
        return restoreConfig;
    }

    public void setRestoreConfig(RestoreConfig restoreConfig) {
        this.restoreConfig = restoreConfig;
    }

    public void setLogConfig(LogConfig logConfig) {
        this.logConfig = logConfig;
    }

    public void setTestConfig(TestConfig testConfig) {
        this.testConfig = testConfig;
    }

    public void setDataTransferConfig(DataTransferConfig dataTransferConfig){
        this.dataTransferConfig = dataTransferConfig;
    }

    public DataTransferConfig getDataTransferConfig() {
        return dataTransferConfig;
    }
}
