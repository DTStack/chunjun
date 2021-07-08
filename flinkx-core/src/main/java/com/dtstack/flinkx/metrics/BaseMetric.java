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


package com.dtstack.flinkx.metrics;

import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.util.SysUtil;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author jiangbo
 * @date 2019/6/5
 */
public class BaseMetric {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    private final static Long DEFAULT_PERIOD_MILLISECONDS = 10000L;

    private Long delayPeriodMill = 12000L;

    private RuntimeContext runtimeContext;

    private MetricGroup flinkxOutput;

    private final Map<String, LongCounter> metricCounters = new HashMap<>();

    public BaseMetric(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;

        flinkxOutput = runtimeContext.getMetricGroup().addGroup(Metrics.METRIC_GROUP_KEY_FLINKX, Metrics.METRIC_GROUP_VALUE_OUTPUT);
        initPeriod();
    }

    public void addMetric(String metricName, LongCounter counter){
        addMetric(metricName, counter, false);
    }

    public void addMetric(String metricName, LongCounter counter, boolean meterView){
        metricCounters.put(metricName, counter);
        flinkxOutput.gauge(metricName, new SimpleAccumulatorGauge<Long>(counter));
        if (meterView){
            flinkxOutput.meter(metricName + Metrics.SUFFIX_RATE, new SimpleLongCounterMeterView(counter, 60));
        }
    }

    public Map<String, LongCounter> getMetricCounters() {
        return metricCounters;
    }

    private void initPeriod() {
        try {
            MetricGroup mgObj = runtimeContext.getMetricGroup();
            Class<AbstractMetricGroup> amgCls = (Class<AbstractMetricGroup>) mgObj.getClass().getSuperclass().getSuperclass();
            Field registryField = amgCls.getDeclaredField("registry");
            registryField.setAccessible(true);
            MetricRegistryImpl registryImplObj = (MetricRegistryImpl) registryField.get(mgObj);
            if (registryImplObj.getReporters().isEmpty()) {
                return;
            }
            Field executorField = registryImplObj.getClass().getDeclaredField("executor");
            executorField.setAccessible(true);
            ScheduledExecutorService executor = (ScheduledExecutorService) executorField.get(registryImplObj);
            Field scheduleField = (executor.getClass().getSuperclass().getDeclaredField("e"));
            scheduleField.setAccessible(true);
            ScheduledThreadPoolExecutor scheduleObj = (ScheduledThreadPoolExecutor) scheduleField.get(executor);
            Runnable runableObj = scheduleObj.getQueue().iterator().next();
            RunnableScheduledFuture runableFuture = (RunnableScheduledFuture) runableObj;
            Field outerTaskField = runableFuture.getClass().getDeclaredField("outerTask");
            outerTaskField.setAccessible(true);
            Object scheduledFutureTask = outerTaskField.get(runableFuture);
            Field periodField = scheduledFutureTask.getClass().getDeclaredField("period");
            periodField.setAccessible(true);
            long schedulePeriod = (long) periodField.get(scheduledFutureTask);
            long schedulePeriodMill = -1 * new FiniteDuration(schedulePeriod, TimeUnit.NANOSECONDS).toMillis();

            LOG.info("InputMetric.scheduledFutureTask.schedulePeriodMill:{} ...", schedulePeriodMill);

            if (schedulePeriodMill > DEFAULT_PERIOD_MILLISECONDS) {
                this.delayPeriodMill = (long) (schedulePeriodMill * 1.2);
            }
        } catch (Exception e) {
            LOG.error("", e);
        }

        LOG.info("InputMetric.delayPeriodMill:{} ...", delayPeriodMill);
    }

    public void waitForReportMetrics() {
        if(delayPeriodMill == 0){
            return;
        }

        try {
            Thread.sleep(delayPeriodMill);

            // 保证只sleep一次
            delayPeriodMill = 0L;
        } catch (InterruptedException e){
            SysUtil.sleep(delayPeriodMill);
            LOG.warn("Task thread is interrupted");
        }
    }
}
