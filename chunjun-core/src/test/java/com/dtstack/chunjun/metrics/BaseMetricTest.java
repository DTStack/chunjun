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

package com.dtstack.chunjun.metrics;

import com.dtstack.chunjun.source.format.MockInputFormat;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BaseMetricTest {

    private static final String TEST_METRIC = "test";
    @TempDir File tempDir;

    private BaseMetric baseMetric;
    private JobMasterGateway jobMasterGateway;

    @BeforeEach
    void setUp() {
        MockInputSplitProvider splitProvider = new MockInputSplitProvider();
        splitProvider.addInputSplits(tempDir.getPath(), 1);

        StringifiedAccumulatorResult[] stringifiedAccumulatorResults =
                ImmutableList.of(new StringifiedAccumulatorResult(TEST_METRIC, "null", "10"))
                        .toArray(new StringifiedAccumulatorResult[0]);
        ArchivedExecutionGraph archivedExecutionGraph =
                new ArchivedExecutionGraph(
                        new JobID(),
                        "DefaultExecutionGraphCacheTest",
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        new long[0],
                        JobStatus.RUNNING,
                        new ErrorInfo(new FlinkException("Test"), 42L),
                        "",
                        stringifiedAccumulatorResults,
                        Collections.emptyMap(),
                        new ArchivedExecutionConfig(new ExecutionConfig()),
                        false,
                        null,
                        null,
                        "stateBackendName",
                        "checkpointStorageName",
                        null,
                        "changelogStorageName");

        this.jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setRequestJobSupplier(
                                () ->
                                        CompletableFuture.completedFuture(
                                                new ExecutionGraphInfo(archivedExecutionGraph)))
                        .build();

        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setInputSplitProvider(splitProvider)
                        .setTaskName("no")
                        .setExecutionConfig(new ExecutionConfig())
                        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
                        .setTaskManagerRuntimeInfo(
                                new MockInputFormat.MockTaskManagerConfiguration())
                        .setAggregateManager(new RpcGlobalAggregateManager(this.jobMasterGateway))
                        .build();
        Map<String, Accumulator<?, ?>> accumulatorMap =
                ImmutableMap.of(TEST_METRIC, new LongCounter(5));
        MockInputFormat.MockRuntimeContext context =
                new MockInputFormat.MockRuntimeContext(environment, accumulatorMap);
        this.baseMetric = new BaseMetric(context);
    }

    @Test
    void addMetric() {}

    @Test
    void testAddMetric() {
        LongCounter longCounter = new LongCounter(0);
        baseMetric.addMetric(TEST_METRIC, longCounter);
        Map<String, LongCounter> metricCounters = baseMetric.getMetricCounters();
        assertEquals(longCounter, metricCounters.get(TEST_METRIC));
    }

    @Test
    void testAddMetricAndMeter() {
        LongCounter longCounter = new LongCounter(0);
        baseMetric.addMetric(TEST_METRIC, longCounter, true);
        Map<String, LongCounter> metricCounters = baseMetric.getMetricCounters();
        assertEquals(longCounter, metricCounters.get(TEST_METRIC));
    }

    @Test
    void addDirtyMetric() {
        LongCounter longCounter = new LongCounter(0);
        baseMetric.addDirtyMetric(TEST_METRIC, longCounter);
        Map<String, LongCounter> metricCounters = baseMetric.getMetricCounters();
        assertEquals(longCounter, metricCounters.get(TEST_METRIC));
    }

    @Test
    void testWaitForReportMetrics() {
        long oldTime = System.currentTimeMillis();
        baseMetric.waitForReportMetrics();
        long newTime = System.currentTimeMillis();
        assertTrue(newTime - oldTime >= BaseMetric.DELAY_PERIOD_MILL);
    }
}
