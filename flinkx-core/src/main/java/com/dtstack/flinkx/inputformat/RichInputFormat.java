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

package com.dtstack.flinkx.inputformat;

import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.metrics.AccumulatorCollector;
import com.dtstack.flinkx.metrics.BaseMetric;
import com.dtstack.flinkx.reader.ByteRateLimiter;
import com.dtstack.flinkx.restore.FormatState;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * FlinkX里面所有自定义inputFormat的抽象基类
 *
 * 扩展了org.apache.flink.api.common.io.RichInputFormat, 因而可以通过{@link #getRuntimeContext()}获取运行时执行上下文
 * 自动完成
 * 用户只需覆盖openInternal,closeInternal等方法, 无需操心细节
 *
 */
public abstract class RichInputFormat extends org.apache.flink.api.common.io.RichInputFormat<Row, InputSplit> {

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

    protected FormatState formatState;

    protected transient BaseMetric inputMetric;

    protected int indexOfSubtask;

    private long startTime;

    protected AccumulatorCollector accumulatorCollector;

    private boolean inited = false;

    protected abstract void openInternal(InputSplit inputSplit) throws IOException;

    @Override
    public void openInputFormat() throws IOException {
        initJobInfo();
        startTime = System.currentTimeMillis();
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        if(!inited){
            initAccumulatorCollector();
            initStatisticsAccumulator();
            openByteRateLimiter();
            initRestoreInfo();

            if(restoreConfig.isRestore()){
                formatState.setNumOfSubTask(indexOfSubtask);
            }

            inited = true;
        }

        openInternal(inputSplit);
    }

    private void initAccumulatorCollector(){
        String lastWriteLocation = String.format("%s_%s", Metrics.LAST_WRITE_LOCATION_PREFIX, indexOfSubtask);
        String lastWriteNum = String.format("%s_%s", Metrics.LAST_WRITE_NUM__PREFIX, indexOfSubtask);

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
            indexOfSubtask = Integer.valueOf(vars.get(Metrics.SUBTASK_INDEX));
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
                formatState = new FormatState(indexOfSubtask, null);
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
        internalRow = setChannelInformation(internalRow);

        updateDuration();
        if(numReadCounter !=null ){
            numReadCounter.add(1);
        }
        if(bytesReadCounter!=null){
            bytesReadCounter.add(internalRow.toString().length());
        }
        return internalRow;
    }

    private Row setChannelInformation(Row internalRow){
        if (internalRow != null){
            Row rowWithChannel = new Row(internalRow.getArity() + 1);
            for (int i = 0; i < internalRow.getArity(); i++) {
                rowWithChannel.setField(i, internalRow.getField(i));
            }

            rowWithChannel.setField(internalRow.getArity(), indexOfSubtask);
            return rowWithChannel;
        }

        return null;
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
        if(durationCounter != null){
            updateDuration();
        }

        if(inputMetric != null){
            inputMetric.waitForReportMetrics();
        }

        if(byteRateLimiter != null){
            byteRateLimiter.stop();
        }

        if(accumulatorCollector != null){
            accumulatorCollector.close();
        }

        LOG.info("subtask input close finished");
    }

    private void updateDuration(){
        if(durationCounter !=null ){
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    protected abstract  void closeInternal() throws IOException;

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    public void setRestoreState(FormatState formatState) {
        this.formatState = formatState;
    }

    public RestoreConfig getRestoreConfig() {
        return restoreConfig;
    }
}
