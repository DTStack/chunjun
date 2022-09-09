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

package com.dtstack.chunjun.source;

import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.source.format.MockInputFormat;
import com.dtstack.chunjun.source.format.MockInputFormatBuilder;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DtInputFormatSourceFunctionTest {

    @TempDir File tempDir;
    private DtInputFormatSourceFunction<RowData> sourceFunction;

    @BeforeEach
    public void setup() {
        MockInputFormatBuilder inputFormatBuilder = new MockInputFormatBuilder();
        BaseRichInputFormat mockInputFormat = inputFormatBuilder.finish();
        MockTypeInfo<RowData> columnRowDataTypeInfo = new MockTypeInfo<>(true);
        this.sourceFunction =
                new DtInputFormatSourceFunction<RowData>(mockInputFormat, columnRowDataTypeInfo);
    }

    @Test
    public void testOpen() {
        MockInputSplitProvider splitProvider = new MockInputSplitProvider();
        splitProvider.addInputSplits(tempDir.getPath(), 1);
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setInputSplitProvider(splitProvider)
                        .setTaskName("no")
                        .setExecutionConfig(new ExecutionConfig())
                        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
                        .setTaskManagerRuntimeInfo(
                                new MockInputFormat.MockTaskManagerConfiguration())
                        .setAggregateManager(
                                new RpcGlobalAggregateManager(
                                        new TestingJobMasterGatewayBuilder().build()))
                        .build();
        this.sourceFunction.setRuntimeContext(new MockInputFormat.MockRuntimeContext(environment));
        Configuration configuration = new Configuration();
        this.sourceFunction.open(configuration);
    }

    @Test
    public void testRun() {
        // reset thread classloader otherwise it will affect other test classes
        classloaderSafeInvoke(
                () -> {
                    MockInputSplitProvider splitProvider = new MockInputSplitProvider();
                    splitProvider.addInputSplits(tempDir.getPath(), 1);
                    MockEnvironment environment =
                            new MockEnvironmentBuilder()
                                    .setInputSplitProvider(splitProvider)
                                    .setTaskName("no")
                                    .setExecutionConfig(new ExecutionConfig())
                                    .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
                                    .setTaskManagerRuntimeInfo(
                                            new MockInputFormat.MockTaskManagerConfiguration())
                                    .setAggregateManager(
                                            new RpcGlobalAggregateManager(
                                                    new TestingJobMasterGatewayBuilder().build()))
                                    .build();
                    this.sourceFunction.setRuntimeContext(
                            new MockInputFormat.MockRuntimeContext(environment));
                    Configuration configuration = new Configuration();
                    this.sourceFunction.open(configuration);
                    CountSourceContext context = new CountSourceContext();
                    this.sourceFunction.run(context);

                    assertEquals(MockInputFormat.SIZE, context.getCount());
                });
    }

    public static void classloaderSafeInvoke(Executable executable) {
        // reset thread classloader otherwise it will affect other test classes
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            executable.execute();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    private static class CountSourceContext implements SourceFunction.SourceContext<RowData> {
        private int count;

        private static final Object LOCK = new Object();

        @Override
        public void collect(RowData element) {
            count++;
        }

        @Override
        public void collectWithTimestamp(RowData element, long timestamp) {}

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void markAsTemporarilyIdle() {}

        @Override
        public Object getCheckpointLock() {
            return LOCK;
        }

        @Override
        public void close() {}

        public int getCount() {
            return count;
        }
    }
}
