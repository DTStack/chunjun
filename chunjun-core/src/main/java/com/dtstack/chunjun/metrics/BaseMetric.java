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

package com.dtstack.chunjun.metrics;

import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.util.ThreadUtil;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class BaseMetric {

    public static Long DELAY_PERIOD_MILL = 20000L;

    public static final String DELAY_PERIOD_MILL_KEY = "chunjun.delay_period_mill";

    private final MetricGroup chunjunMetricGroup;

    private final MetricGroup chunjunDirtyMetricGroup;

    private final Map<String, LongCounter> metricCounters = new HashMap<>();

    public BaseMetric(RuntimeContext runtimeContext) {

        ExecutionConfig.GlobalJobParameters params =
                runtimeContext.getExecutionConfig().getGlobalJobParameters();
        Map<String, String> confMap = params.toMap();
        this.DELAY_PERIOD_MILL =
                Long.parseLong(
                        String.valueOf(confMap.getOrDefault(DELAY_PERIOD_MILL_KEY, "20000")));

        chunjunMetricGroup =
                runtimeContext
                        .getMetricGroup()
                        .addGroup(
                                Metrics.METRIC_GROUP_KEY_CHUNJUN,
                                Metrics.METRIC_GROUP_VALUE_OUTPUT);

        chunjunDirtyMetricGroup =
                chunjunMetricGroup.addGroup(
                        Metrics.METRIC_GROUP_KEY_DIRTY, Metrics.METRIC_GROUP_VALUE_OUTPUT);
    }

    public void addMetric(String metricName, LongCounter counter) {
        addMetric(metricName, counter, false);
    }

    public void addMetric(String metricName, LongCounter counter, boolean meterView) {
        metricCounters.put(metricName, counter);
        chunjunMetricGroup.gauge(metricName, new SimpleAccumulatorGauge<>(counter));
        if (meterView) {
            chunjunMetricGroup.meter(
                    metricName + Metrics.SUFFIX_RATE, new SimpleLongCounterMeterView(counter, 20));
        }
    }

    public void addDirtyMetric(String metricName, LongCounter counter) {
        metricCounters.put(metricName, counter);
        chunjunDirtyMetricGroup.gauge(metricName, new SimpleAccumulatorGauge<>(counter));
    }

    public Map<String, LongCounter> getMetricCounters() {
        return metricCounters;
    }

    public void waitForReportMetrics() {
        try {
            Thread.sleep(DELAY_PERIOD_MILL);
        } catch (InterruptedException e) {
            ThreadUtil.sleepMilliseconds(DELAY_PERIOD_MILL);
            log.warn("Task thread is interrupted");
        }
    }

    public MetricGroup getChunjunMetricGroup() {
        return chunjunMetricGroup;
    }
}
