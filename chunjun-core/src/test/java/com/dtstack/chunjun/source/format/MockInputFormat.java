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

package com.dtstack.chunjun.source.format;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.dirty.utils.DirtyConfUtil;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MockInputFormat extends BaseRichInputFormat {

    protected static final String MOCK_JOB_NAME = "mock_job";

    public static final RowData ERROR_DATA = new ColumnRowData(1);
    public static final RowData SUCCESS_DATA = new ColumnRowData(1);

    public static final int SIZE = 10;

    private int count = 0;

    public MockInputFormat() {
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setTaskName("no")
                        .setExecutionConfig(new ExecutionConfig())
                        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
                        .setTaskManagerRuntimeInfo(new MockTaskManagerConfiguration())
                        .setAggregateManager(
                                new RpcGlobalAggregateManager(
                                        new TestingJobMasterGatewayBuilder().build()))
                        .build();
        setRuntimeContext(new MockRuntimeContext(environment));
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setMetricPluginName("mock");
        setConfig(commonConfig);
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        if (minNumSplits == 0) {
            throw new IllegalArgumentException("minNumSplits must be large than 0");
        } else {
            InputSplit[] inputSplits = new InputSplit[minNumSplits];
            for (int i = 0; i < minNumSplits; i++) {
                inputSplits[i] = new MockSplit(i, minNumSplits);
            }
            return inputSplits;
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {}

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        count++;
        if (SUCCESS_DATA == rowData) {
            return rowData;
        } else {
            throw new ReadRecordException("error data", new RuntimeException());
        }
    }

    @Override
    protected void closeInternal() throws IOException {}

    @Override
    public boolean reachedEnd() throws IOException {
        return count >= SIZE;
    }

    @Override
    protected boolean useCustomReporter() {
        return true;
    }

    public static class MockRuntimeContext extends StreamingRuntimeContext {

        public MockRuntimeContext(Environment environment) {
            super(new MockStreamOperator(), environment, new HashMap<String, Accumulator<?, ?>>());
            Configuration configuration = new Configuration();
            configuration.setString(DirtyConfUtil.TYPE_KEY, "mock");
            this.getExecutionConfig().setGlobalJobParameters(configuration);
        }

        public MockRuntimeContext(
                Environment environment, Map<String, Accumulator<?, ?>> accumulatorMap) {
            super(new MockStreamOperator(), environment, accumulatorMap);
            Configuration configuration = new Configuration();
            configuration.setString(DirtyConfUtil.TYPE_KEY, "mock");
            this.getExecutionConfig().setGlobalJobParameters(configuration);
        }
    }

    private static class MockStreamOperator extends AbstractStreamOperator<Integer> {
        private static final long serialVersionUID = -1153976702711944427L;

        @Override
        public ExecutionConfig getExecutionConfig() {
            return new ExecutionConfig();
        }

        @Override
        public OperatorID getOperatorID() {
            return new OperatorID();
        }

        @Override
        public OperatorMetricGroup getMetricGroup() {
            MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
            return new OperatorMetricGroup() {
                @Override
                public OperatorIOMetricGroup getIOMetricGroup() {
                    return null;
                }

                @Override
                public Counter counter(String s) {
                    return null;
                }

                @Override
                public <C extends Counter> C counter(String s, C c) {
                    return null;
                }

                @Override
                public <T, G extends Gauge<T>> G gauge(String s, G g) {
                    return null;
                }

                @Override
                public <H extends Histogram> H histogram(String s, H h) {
                    return null;
                }

                @Override
                public <M extends Meter> M meter(String s, M m) {
                    return null;
                }

                @Override
                public MetricGroup addGroup(String s) {
                    return null;
                }

                @Override
                public MetricGroup addGroup(String s, String s1) {
                    return null;
                }

                @Override
                public String[] getScopeComponents() {
                    return new String[0];
                }

                @Override
                public Map<String, String> getAllVariables() {
                    return null;
                }

                @Override
                public String getMetricIdentifier(String s) {
                    return null;
                }

                @Override
                public String getMetricIdentifier(String s, CharacterFilter characterFilter) {
                    return null;
                }
            };
        }
    }

    private static class MockMetricGroup extends AbstractMetricGroup<AbstractMetricGroup<?>> {
        private String name;

        public MockMetricGroup(MetricRegistry registry, AbstractMetricGroup parent, String name) {
            super(registry, makeScopeComponents(parent, name), parent);
            this.name = name;
        }

        protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
            return this.parent
                    .getQueryServiceMetricInfo(filter)
                    .copy(filter.filterCharacters(this.name));
        }

        private static String[] makeScopeComponents(AbstractMetricGroup parent, String name) {
            if (parent != null) {
                String[] parentComponents = parent.getScopeComponents();
                if (parentComponents != null && parentComponents.length > 0) {
                    String[] parts = new String[parentComponents.length + 1];
                    System.arraycopy(parentComponents, 0, parts, 0, parentComponents.length);
                    parts[parts.length - 1] = name;
                    return parts;
                }
            }

            return new String[] {name};
        }

        protected String getGroupName(CharacterFilter filter) {
            return filter.filterCharacters(this.name);
        }

        @Override
        protected void putVariables(Map<String, String> variables) {
            variables.put(Metrics.JOB_NAME, MOCK_JOB_NAME);
            variables.put(Metrics.SUBTASK_INDEX, "1");
        }
    }

    public static class MockTaskManagerConfiguration extends TaskManagerConfiguration {

        public MockTaskManagerConfiguration() {
            super(
                    1,
                    ResourceProfile.ANY,
                    ResourceProfile.ANY,
                    new String[] {"/tmp/dir"},
                    Time.of(5000, TimeUnit.MILLISECONDS),
                    Time.of(60000, TimeUnit.MILLISECONDS),
                    null,
                    new Configuration(),
                    true,
                    "/tmp/dir/log/temp.log",
                    "/tmp/dir/log/stdout.log",
                    "/tmp/dir/log",
                    "",
                    new File("/tmp/dir/"),
                    new RetryingRegistrationConfiguration(
                            20L, 1000L, 15000L, // make sure that we timeout in case of an error
                            15000L));
        }
    }
}
