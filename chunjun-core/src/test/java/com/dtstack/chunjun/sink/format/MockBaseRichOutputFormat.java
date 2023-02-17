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

package com.dtstack.chunjun.sink.format;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.source.format.MockInputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

public class MockBaseRichOutputFormat extends BaseRichOutputFormat {

    protected static final long SUCCESS_CHECK_POINT_ID = 1L;
    protected static final long FAIL_CHECK_POINT_ID = 2L;

    public MockBaseRichOutputFormat() {
        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setTaskName("no")
                        .setExecutionConfig(new ExecutionConfig())
                        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
                        .setTaskManagerRuntimeInfo(
                                new MockInputFormat.MockTaskManagerConfiguration())
                        .setAggregateManager(
                                new RpcGlobalAggregateManager(
                                        new TestingJobMasterGatewayBuilder().build()))
                        .build();
        setRuntimeContext(new MockInputFormat.MockRuntimeContext(environment));
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setMetricPluginName("mock");
        setConfig(commonConfig);
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {}

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {}

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {}

    @Override
    protected void closeInternal() throws IOException {}

    @Override
    public void commit(long checkpointId) throws Exception {
        if (FAIL_CHECK_POINT_ID == checkpointId) {
            throw new RuntimeException("commit check point fail");
        }
    }

    @Override
    public void rollback(long checkpointId) throws Exception {
        if (FAIL_CHECK_POINT_ID == checkpointId) {
            throw new RuntimeException("rollback check point fail");
        }
    }
}
