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

import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.dirty.manager.DirtyManager;
import com.dtstack.chunjun.metrics.AccumulatorCollector;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/** A {@link SourceFunction} that reads data using an {@link InputFormat}. */
@Slf4j
public class DtInputFormatSourceFunction<OUT> extends InputFormatSourceFunction<OUT>
        implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    private final TypeInformation<OUT> typeInfo;
    private transient TypeSerializer<OUT> serializer;

    private final InputFormat<OUT, InputSplit> format;

    private transient InputSplitProvider provider;
    private transient Iterator<InputSplit> splitIterator;

    private volatile boolean isRunning = true;

    private Map<Integer, FormatState> formatStateMap;

    private static final String LOCATION_STATE_NAME = "data-sync-location-states";

    private transient ListState<FormatState> unionOffsetStates;

    @SuppressWarnings("unchecked")
    public DtInputFormatSourceFunction(InputFormat<OUT, ?> format, TypeInformation<OUT> typeInfo) {
        super(format, typeInfo);
        this.format = (InputFormat<OUT, InputSplit>) format;
        this.typeInfo = typeInfo;
    }

    @Override
    public void open(Configuration parameters) {
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

        if (format instanceof RichInputFormat) {
            ((RichInputFormat) format).setRuntimeContext(context);
        }

        if (format instanceof BaseRichInputFormat) {
            if (formatStateMap != null) {
                ((BaseRichInputFormat) format)
                        .setFormatState(formatStateMap.get(context.getIndexOfThisSubtask()));
            }
        }

        format.configure(parameters);

        provider = context.getInputSplitProvider();
        serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
        splitIterator = getInputSplits();
        isRunning = splitIterator.hasNext();
    }

    @Override
    public void run(SourceContext<OUT> ctx) throws Exception {
        Exception tryException = null;
        try {

            Counter completedSplitsCounter =
                    getRuntimeContext().getMetricGroup().counter("numSplitsProcessed");
            if (isRunning && format instanceof RichInputFormat) {
                ((RichInputFormat) format).openInputFormat();
            }
            OUT nextElement = serializer.createInstance();
            while (isRunning) {
                format.open(splitIterator.next());
                AccumulatorCollector accumulatorCollector =
                        ((BaseRichInputFormat) format).getAccumulatorCollector();
                DirtyManager dirtyManager = ((BaseRichInputFormat) format).getDirtyManager();
                // for each element we also check if cancel
                // was called by checking the isRunning flag

                while (isRunning && !format.reachedEnd()) {
                    synchronized (ctx.getCheckpointLock()) {
                        try {
                            nextElement = format.nextRecord(nextElement);
                            if (nextElement != null) {
                                ctx.collect(nextElement);
                            }
                        } catch (Exception e) {
                            // 脏数据总数应是所有slot的脏数据总数，而不是单个的
                            long globalErrors =
                                    accumulatorCollector.getAccumulatorValue(
                                            Metrics.NUM_ERRORS, false);
                            dirtyManager.collect(nextElement, e, null, globalErrors);
                        }
                    }
                }
                format.close();
                completedSplitsCounter.inc();

                if (isRunning) {
                    isRunning = splitIterator.hasNext();
                }
            }
        } catch (Exception exception) {
            tryException = exception;
            log.error("Exception happened, start to close format", exception);
        } finally {
            isRunning = false;
            gracefulClose();
            if (null != tryException) {
                throw tryException;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        gracefulClose();
    }

    private void gracefulClose() {
        try {
            format.close();
        } catch (IOException e) {
            log.warn(ExceptionUtil.getErrorMessage(e));
        }

        if (format instanceof RichInputFormat) {
            try {
                ((RichInputFormat) format).closeInputFormat();
            } catch (IOException e) {
                log.error(ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /**
     * Returns the {@code InputFormat}. This is only needed because we need to set the input split
     * assigner on the {@code StreamGraph}.
     */
    @Override
    public InputFormat<OUT, InputSplit> getFormat() {
        return format;
    }

    private Iterator<InputSplit> getInputSplits() {

        return new Iterator<InputSplit>() {

            private InputSplit nextSplit;

            private boolean exhausted;

            @Override
            public boolean hasNext() {
                if (exhausted) {
                    return false;
                }

                if (nextSplit != null) {
                    return true;
                }

                final InputSplit split;
                try {
                    split =
                            provider.getNextInputSplit(
                                    getRuntimeContext().getUserCodeClassLoader());
                } catch (InputSplitProviderException e) {
                    throw new RuntimeException("Could not retrieve next input split.", e);
                }

                if (split != null) {
                    this.nextSplit = split;
                    return true;
                } else {
                    exhausted = true;
                    return false;
                }
            }

            @Override
            public InputSplit next() {
                if (this.nextSplit == null && !hasNext()) {
                    throw new NoSuchElementException();
                }

                final InputSplit tmp = this.nextSplit;
                this.nextSplit = null;
                return tmp;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        FormatState formatState = ((BaseRichInputFormat) format).getFormatState();
        if (formatState != null) {
            log.info("InputFormat format state:{}", formatState);
            unionOffsetStates.clear();
            unionOffsetStates.add(formatState);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore stateStore = context.getOperatorStateStore();
        log.info("Start initialize input format state, is restored:{}", context.isRestored());
        unionOffsetStates =
                stateStore.getUnionListState(
                        new ListStateDescriptor<>(
                                LOCATION_STATE_NAME,
                                TypeInformation.of(new TypeHint<FormatState>() {})));
        if (context.isRestored()) {
            formatStateMap = new HashMap<>(16);
            for (FormatState formatState : unionOffsetStates.get()) {
                formatStateMap.put(formatState.getNumOfSubTask(), formatState);
                log.info("Input format state into:{}", formatState);
            }
        }
        log.info("End initialize input format state");
    }
}
