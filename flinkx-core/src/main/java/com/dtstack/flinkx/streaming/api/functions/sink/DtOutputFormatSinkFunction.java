/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.streaming.api.functions.sink;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
/**
 * Simple implementation of the SinkFunction writing tuples in the specified
 * OutputFormat format.
 *
 * @param <IN> Input type
 *
 * @deprecated Please use the {@code BucketingSink} for writing to files from a streaming program.
 *
 * @author jiangbo
 */
@PublicEvolving
@Deprecated
public class DtOutputFormatSinkFunction<IN> extends OutputFormatSinkFunction<IN> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DtOutputFormatSinkFunction.class);

    private OutputFormat<IN> format;
    private boolean cleanupCalled = false;

    private static final String LOCATION_STATE_NAME = "data-sync-location-states";

    private transient ListState<FormatState> unionOffsetStates;

    private Map<Integer,FormatState> formatStateMap;

    public DtOutputFormatSinkFunction(OutputFormat<IN> format) {
        super(format);
        this.format = format;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext context = getRuntimeContext();
        format.configure(parameters);

        if (format instanceof BaseRichOutputFormat && formatStateMap != null){
            ((BaseRichOutputFormat) format).setRestoreState(formatStateMap.get(context.getIndexOfThisSubtask()));
        }

        int indexInSubtaskGroup = context.getIndexOfThisSubtask();
        int currentNumberOfSubtasks = context.getNumberOfParallelSubtasks();
        format.open(indexInSubtaskGroup, currentNumberOfSubtasks);
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {
        super.setRuntimeContext(context);
        if (format instanceof RichOutputFormat) {
            ((RichOutputFormat) format).setRuntimeContext(context);
        }
    }

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        if (format instanceof InputTypeConfigurable) {
            InputTypeConfigurable itc = (InputTypeConfigurable) format;
            itc.setInputType(type, executionConfig);
        }
    }

    @Override
    public void invoke(IN record) throws Exception {
        try {
            format.writeRecord(record);
        } catch (Exception ex) {
            cleanup();
            throw ex;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            format.close();
        } catch (Exception ex) {
            cleanup();
            throw ex;
        }
    }

    private void cleanup() {
        try {
            if (!cleanupCalled && format instanceof CleanupWhenUnsuccessful) {
                cleanupCalled = true;
                ((CleanupWhenUnsuccessful) format).tryCleanupOnError();
            }
        } catch (Throwable t) {
            LOG.error("Cleanup on error failed.", t);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        FormatState formatState = ((BaseRichOutputFormat) format).getFormatState();
        if (formatState != null){
            LOG.info("OutputFormat format state:{}", formatState);
            unionOffsetStates.clear();
            unionOffsetStates.add(formatState);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("Start initialize output format state");

        OperatorStateStore stateStore = context.getOperatorStateStore();
        unionOffsetStates = stateStore.getUnionListState(new ListStateDescriptor<>(
                LOCATION_STATE_NAME,
                TypeInformation.of(new TypeHint<FormatState>() {})));

        LOG.info("Is restored:{}", context.isRestored());
        if (context.isRestored()){
            formatStateMap = new HashMap<>(16);
            for (FormatState formatState : unionOffsetStates.get()) {
                formatStateMap.put(formatState.getNumOfSubTask(), formatState);
                LOG.info("Output format state into:{}" ,formatState.toString());
            }
        }

        LOG.info("End initialize output format state");
    }
}
