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
import com.dtstack.flinkx.metrics.BaseMetric;
import com.dtstack.flinkx.reader.ByteRateLimiter;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.LimitedQueue;
import com.dtstack.flinkx.util.SysUtil;
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
    protected LongCounter numReadCounter;
    protected String monitorUrls;
    protected long bytes;
    protected ByteRateLimiter byteRateLimiter;

    protected RestoreConfig restoreConfig;

    protected FormatState formatState;

    protected transient BaseMetric inputMetric;

    protected int indexOfSubtask;

    private LimitedQueue<Object> limitedQueue;

    protected abstract void openInternal(InputSplit inputSplit) throws IOException;

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        Map<String, String> vars = getRuntimeContext().getMetricGroup().getAllVariables();
        if (vars != null && vars.get(Metrics.JOB_NAME) != null) {
            jobName = vars.get(Metrics.JOB_NAME);
        }

        numReadCounter = getRuntimeContext().getLongCounter(Metrics.NUM_READS);
        indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();

        inputMetric = new BaseMetric(getRuntimeContext(), "reader");
        inputMetric.addMetric(Metrics.NUM_READS, numReadCounter);

        openInternal(inputSplit);

        if (StringUtils.isNotBlank(this.monitorUrls) && this.bytes > 0) {
            this.byteRateLimiter = new ByteRateLimiter(getRuntimeContext(), this.monitorUrls, this.bytes, 2);
            this.byteRateLimiter.start();
        }

        if(restoreConfig == null){
            restoreConfig = RestoreConfig.defaultConfig();
        } else if(restoreConfig.isRestore()){
            if(formatState == null){
                formatState = new FormatState(indexOfSubtask, null);
            } else {
                numReadCounter.add(formatState.getNumberRead());
            }

            limitedQueue = new LimitedQueue<>(restoreConfig.getCacheQueueSize());
        }
    }

    @Override
    public Row nextRecord(Row row) throws IOException {
        numReadCounter.add(1);

        if(byteRateLimiter != null) {
            byteRateLimiter.acquire();
        }

        /*
         * Append channel information after the data
         */
        Row internalRow = nextRecordInternal(row);
        if (internalRow != null){
            if (restoreConfig.isRestore()){
                limitedQueue.offer(internalRow.getField(restoreConfig.getRestoreColumnIndex()));
            }

            Row rowWithChannel = new Row(internalRow.getArity() + 1);
            for (int i = 0; i < internalRow.getArity(); i++) {
                rowWithChannel.setField(i, internalRow.getField(i));
            }

            rowWithChannel.setField(internalRow.getArity(), indexOfSubtask);
            return rowWithChannel;
        } else {
            return null;
        }
    }

    /**
     * Get the recover point of current channel
     * @return DataRecoverPoint
     */
    public FormatState getFormatState(){
        formatState.setState(limitedQueue.poll());
        formatState.setNumberRead(numReadCounter.getLocalValue() - limitedQueue.size());
        return formatState;
    }

    protected abstract Row nextRecordInternal(Row row) throws IOException;

    @Override
    public void close() throws IOException {
        try{
            closeInternal();

            inputMetric.waitForReportMetrics();
        }catch (Exception e){
            throw new RuntimeException(e);
        }finally {
            if(byteRateLimiter != null) {
                byteRateLimiter.stop();
                byteRateLimiter = null;
            }
            LOG.info("subtask input close finished");
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
