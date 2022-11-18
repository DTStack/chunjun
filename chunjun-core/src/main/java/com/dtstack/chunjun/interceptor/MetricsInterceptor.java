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

package com.dtstack.chunjun.interceptor;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.metrics.AccumulatorCollector;
import com.dtstack.chunjun.metrics.BaseMetric;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.ByteRateLimiter;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import java.io.IOException;
import java.util.Arrays;

public class MetricsInterceptor implements Interceptor {

    private AccumulatorCollector accumulatorCollector;
    /** checkpoint状态缓存map */
    private FormatState formatState;

    private LongCounter numReadCounter;
    private LongCounter bytesReadCounter;
    private LongCounter durationCounter;
    private ByteRateLimiter byteRateLimiter;
    private BaseMetric inputMetric;

    private long startTime;

    private final StreamingRuntimeContext context;
    private final int indexOfSubTask;
    private final CommonConfig config;

    public MetricsInterceptor(
            StreamingRuntimeContext context, int indexOfSubTask, CommonConfig conf) {
        this.context = context;
        this.indexOfSubTask = indexOfSubTask;
        this.config = conf;
    }

    @Override
    public void init(Configuration configuration) {
        initAccumulatorCollector();
        initStatisticsAccumulator();
        initByteRateLimiter();
        initRestoreInfo();
        this.startTime = System.currentTimeMillis();
    }

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
        numReadCounter = context.getLongCounter(Metrics.NUM_READS);
        bytesReadCounter = context.getLongCounter(Metrics.READ_BYTES);
        durationCounter = context.getLongCounter(Metrics.READ_DURATION);

        inputMetric = new BaseMetric(context);
        inputMetric.addMetric(Metrics.NUM_READS, numReadCounter, true);
        inputMetric.addMetric(Metrics.READ_BYTES, bytesReadCounter, true);
        inputMetric.addMetric(Metrics.READ_DURATION, durationCounter);
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

    @Override
    public void pre(Context context) {
        if (byteRateLimiter != null) {
            byteRateLimiter.acquire();
        }
    }

    @Override
    public void post(Context context) {
        if (context.get("data", RowData.class) != null) {
            updateDuration();
            if (numReadCounter != null) {
                numReadCounter.add(1);
            }
            if (bytesReadCounter != null) {
                bytesReadCounter.add(
                        ObjectSizeCalculator.getObjectSize(context.get("data", RowData.class)));
            }
        }
    }

    @Override
    public void close() throws IOException {

        if (durationCounter != null) {
            updateDuration();
        }

        if (byteRateLimiter != null) {
            byteRateLimiter.stop();
        }

        if (accumulatorCollector != null) {
            accumulatorCollector.close();
        }

        if (inputMetric != null) {
            inputMetric.waitForReportMetrics();
        }
    }

    private void updateDuration() {
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }
}
