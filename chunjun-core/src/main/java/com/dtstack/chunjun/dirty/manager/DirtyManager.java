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

package com.dtstack.chunjun.dirty.manager;

import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.dirty.consumer.DirtyDataCollector;
import com.dtstack.chunjun.dirty.impl.DirtyDataEntry;
import com.dtstack.chunjun.factory.ChunJunThreadFactory;
import com.dtstack.chunjun.util.DataSyncFactoryUtil;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DirtyManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String JOB_ID = "<job_id>";

    private static final String JOB_NAME = "<job_name>";

    private static final String OPERATOR_NAME = "<operator_name>";

    private static final int MAX_THREAD_POOL_SIZE = 1;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Gson GSON =
            new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    private transient ThreadPoolExecutor executor;

    private final AtomicBoolean isAlive = new AtomicBoolean(true);

    private DirtyDataCollector consumer;

    private final String jobId;

    private final String jobName;

    private final String operationName;

    private final LongCounter errorCounter;

    public DirtyManager(DirtyConfig dirtyConfig, RuntimeContext runtimeContext) {
        this.consumer = DataSyncFactoryUtil.discoverDirty(dirtyConfig);
        Map<String, String> allVariables = runtimeContext.getMetricGroup().getAllVariables();
        this.jobId = allVariables.get(JOB_ID);
        this.jobName = allVariables.getOrDefault(JOB_NAME, "defaultJobName");
        this.operationName = allVariables.getOrDefault(OPERATOR_NAME, "defaultOperatorName");
        this.errorCounter = runtimeContext.getLongCounter(Metrics.NUM_ERRORS);
    }

    public void execute() {
        if (executor == null) {
            executor =
                    new ThreadPoolExecutor(
                            MAX_THREAD_POOL_SIZE,
                            MAX_THREAD_POOL_SIZE,
                            0,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(),
                            new ChunJunThreadFactory(
                                    "dirty-consumer",
                                    true,
                                    (t, e) -> {
                                        log.error(
                                                String.format(
                                                        "Thread [%s] consume failed.", t.getName()),
                                                e);
                                    }),
                            new ThreadPoolExecutor.CallerRunsPolicy());
        }

        consumer.open();
        executor.execute(consumer);
    }

    public LongCounter getConsumedMetric() {
        return consumer.getConsumed();
    }

    public LongCounter getFailedConsumedMetric() {
        return consumer.getFailedConsumed();
    }

    public void collect(Object data, Throwable cause, String field, long globalErrors) {
        if (executor == null) {
            execute();
        }

        DirtyDataEntry entity = new DirtyDataEntry();

        entity.setJobId(jobId);
        entity.setJobName(jobName);
        entity.setOperatorName(operationName);
        entity.setCreateTime(new Timestamp(System.currentTimeMillis()));
        entity.setDirtyContent(toString(data));
        entity.setFieldName(field);
        entity.setErrorMessage(ExceptionUtil.getErrorMessage(cause));

        errorCounter.add(1L);
        consumer.offer(entity, globalErrors);
    }

    public String toString(Object data) {
        try {
            return OBJECT_MAPPER.writeValueAsString(data);
        } catch (Exception e) {
            try {
                return GSON.toJson(data);
            } catch (Exception processingException) {
                log.warn("Dirty transform to String failed.", processingException);
                return null;
            }
        }
    }

    /** Close manager. */
    public void close() {
        if (!isAlive.get()) {
            return;
        }

        if (consumer != null) {
            consumer.close();
        }

        if (executor != null) {
            executor.shutdown();
        }

        isAlive.compareAndSet(true, false);
    }

    public void setConsumer(DirtyDataCollector consumer) {
        this.consumer = consumer;
    }

    public boolean isAlive() {
        return isAlive.get();
    }
}
