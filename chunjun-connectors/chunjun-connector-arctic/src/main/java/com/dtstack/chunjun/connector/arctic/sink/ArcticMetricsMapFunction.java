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

package com.dtstack.chunjun.connector.arctic.sink;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.metrics.AccumulatorCollector;
import com.dtstack.chunjun.metrics.BaseMetric;
import com.dtstack.chunjun.metrics.RowSizeCalculator;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ArcticMetricsMapFunction extends RichMapFunction<RowData, RowData> {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    /** 输出指标组 */
    protected transient BaseMetric outputMetric;
    /** 环境上下文 */
    protected StreamingRuntimeContext context;
    /** 累加器收集器 */
    protected AccumulatorCollector accumulatorCollector;
    /** 对象大小计算器 */
    protected RowSizeCalculator<RowData> rowSizeCalculator;

    protected LongCounter bytesWriteCounter;
    protected LongCounter durationCounter;
    protected LongCounter numWriteCounter;
    protected LongCounter snapshotWriteCounter;
    protected LongCounter errCounter;
    protected LongCounter nullErrCounter;
    protected LongCounter duplicateErrCounter;
    protected LongCounter conversionErrCounter;
    protected LongCounter otherErrCounter;

    /** 任务开始时间, openInputFormat()开始计算 */
    protected long startTime;
    /** 任务公共配置 */
    protected CommonConfig config;

    /** 任务名称 */
    protected String jobName = "defaultJobName";
    /** 任务id */
    protected String jobId;
    /** 任务索引id */
    protected int taskNumber;
    /** 子任务数量 */
    protected int numTasks;

    public ArcticMetricsMapFunction(CommonConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        context = (StreamingRuntimeContext) getRuntimeContext();
        numTasks = context.getNumberOfParallelSubtasks();
        taskNumber = context.getIndexOfThisSubtask();
        this.startTime = System.currentTimeMillis();

        Map<String, String> vars = context.getMetricGroup().getAllVariables();
        if (vars != null) {
            jobName = vars.getOrDefault(Metrics.JOB_NAME, "defaultJobName");
            jobId = vars.get(Metrics.JOB_ID);
        }

        initStatisticsAccumulator();
        initAccumulatorCollector();
        initRowSizeCalculator();
    }

    @Override
    public RowData map(RowData rowData) {
        numWriteCounter.add(1L);
        updateDuration();
        bytesWriteCounter.add(rowSizeCalculator.getObjectSize(rowData));
        return rowData;
    }

    @Override
    public void close() throws Exception {
        updateDuration();
        if (outputMetric != null) {
            outputMetric.waitForReportMetrics();
        }
        if (accumulatorCollector != null) {
            accumulatorCollector.close();
        }
        LOG.info("subtask[{}}] close() finished", taskNumber);

        super.close();
    }

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
    }

    /** 初始化累加器收集器 */
    private void initAccumulatorCollector() {
        accumulatorCollector = new AccumulatorCollector(context, Metrics.METRIC_SINK_LIST);
        accumulatorCollector.start();
    }

    /** 更新任务执行时间指标 */
    protected void updateDuration() {
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    /** 初始化对象大小计算器 */
    protected void initRowSizeCalculator() {
        rowSizeCalculator =
                RowSizeCalculator.getRowSizeCalculator(config.getRowSizeCalculatorType(), true);
    }
}
