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
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.ReflectionUtils;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AccumulatorCollector {

    private static final String THREAD_NAME = "accumulator-collector-thread";

    protected static final int MAX_COLLECT_ERROR_TIMES = 100;
    private long collectErrorTimes = 0;

    private JobMasterGateway gateway;

    private final long period;

    protected final ScheduledExecutorService scheduledExecutorService;
    private final Map<String, ValueAccumulator> valueAccumulatorMap;

    private String rdbMaxFuncValue = Metrics.MAX_VALUE_NONE;

    public AccumulatorCollector(StreamingRuntimeContext context, List<String> metricNames) {
        Preconditions.checkArgument(metricNames != null && metricNames.size() > 0);
        valueAccumulatorMap = new HashMap<>(metricNames.size());
        for (String metricName : metricNames) {
            valueAccumulatorMap.put(
                    metricName, new ValueAccumulator(context.getLongCounter(metricName), 0));
        }

        scheduledExecutorService =
                new ScheduledThreadPoolExecutor(1, r -> new Thread(r, THREAD_NAME));

        // 比task manager心跳间隔多1秒
        this.period =
                ((TaskManagerConfiguration) context.getTaskManagerRuntimeInfo())
                                .getRpcTimeout()
                                .toMilliseconds()
                        + 1000;
        RpcGlobalAggregateManager globalAggregateManager =
                ((RpcGlobalAggregateManager) (context).getGlobalAggregateManager());
        Field field = ReflectionUtils.getDeclaredField(globalAggregateManager, "jobMasterGateway");
        assert field != null;
        field.setAccessible(true);
        try {
            gateway = (JobMasterGateway) field.get(globalAggregateManager);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            log.error(
                    "failed to get field:[gateway] from RpcGlobalAggregateManager, e = {}",
                    ExceptionUtil.getErrorMessage(e));
        }
    }

    /** 启动线程池，周期性更新累加器信息 */
    public void start() {
        scheduledExecutorService.scheduleAtFixedRate(
                this::collectAccumulator, 0, period, TimeUnit.MILLISECONDS);
    }

    /** 关闭线程池 */
    public void close() {
        if (scheduledExecutorService != null
                && !scheduledExecutorService.isShutdown()
                && !scheduledExecutorService.isTerminated()) {
            scheduledExecutorService.shutdown();
        }
    }

    /** 收集累加器信息 */
    public void collectAccumulator() {
        CompletableFuture<ExecutionGraphInfo> executionGraphInfoCompletableFuture =
                gateway.requestJob(Time.seconds(10));
        ExecutionGraphInfo executionGraphInfo;
        try {
            executionGraphInfo = executionGraphInfoCompletableFuture.get();
        } catch (Exception e) {
            // 限制最大出错次数，超过最大次数则使任务失败，如果不失败，统计数据没有及时更新，会影响速率限制，错误控制等功能
            collectErrorTimes++;
            if (collectErrorTimes > MAX_COLLECT_ERROR_TIMES) {
                // 主动关闭线程和资源，防止异常情况下没有关闭
                close();
                throw new RuntimeException(
                        "The number of errors in updating statistics data exceeds the maximum limit of 100 times. To ensure the correctness of the data, the task automatically fails");
            }
            return;
        }
        StringifiedAccumulatorResult[] accumulatorResult =
                executionGraphInfo.getArchivedExecutionGraph().getAccumulatorResultsStringified();
        for (StringifiedAccumulatorResult result : accumulatorResult) {
            ValueAccumulator valueAccumulator = valueAccumulatorMap.get(result.getName());
            if (valueAccumulator != null) {
                valueAccumulator.setGlobal(Long.parseLong(result.getValue()));
            } else if (result.getName().equals(Metrics.MAX_VALUE)) {
                rdbMaxFuncValue = result.getValue();
            }
        }
    }

    public String getRdbMaxFuncValue() {
        waited();
        return rdbMaxFuncValue;
    }

    /**
     * 获取指定累加器信息
     *
     * @param name 累加器名称
     * @param needWaited 是否需要等待
     * @return
     */
    public long getAccumulatorValue(String name, boolean needWaited) {
        if (needWaited) {
            waited();
        }
        ValueAccumulator valueAccumulator = valueAccumulatorMap.get(name);
        if (valueAccumulator == null) {
            return 0;
        }
        return valueAccumulator.getGlobal();
    }

    private void waited() {
        try {
            TimeUnit.MILLISECONDS.sleep(this.period);
        } catch (InterruptedException e) {
            log.warn(
                    "Interrupted when waiting for valueAccumulatorMap, e = {}",
                    ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 根据名称获取指定累加器的本地value
     *
     * @param name 累加器指标名称
     * @return
     */
    public long getLocalAccumulatorValue(String name) {
        ValueAccumulator valueAccumulator = valueAccumulatorMap.get(name);
        if (valueAccumulator == null) {
            return 0;
        }
        return valueAccumulator.getLocal().getLocalValue();
    }
}
