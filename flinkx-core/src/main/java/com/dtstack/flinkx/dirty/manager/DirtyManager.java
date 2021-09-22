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

package com.dtstack.flinkx.dirty.manager;

import com.dtstack.flinkx.dirty.DirtyConf;
import com.dtstack.flinkx.dirty.consumer.DirtyDataCollector;
import com.dtstack.flinkx.dirty.impl.DirtyDataEntry;
import com.dtstack.flinkx.dirty.utils.DirtyConfUtil;
import com.dtstack.flinkx.factory.FlinkxThreadFactory;
import com.dtstack.flinkx.util.DataSyncFactoryUtil;
import com.dtstack.flinkx.util.ExceptionUtil;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tiezhu@dtstack
 * @date 22/09/2021 Wednesday
 */
public class DirtyManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DirtyManager.class);

    private static final String JOB_ID = "<job_id>";

    private static final String JOB_NAME = "<job_name>";

    private static final String OPERATOR_NAME = "<operator_name>";

    private static final int MAX_THREAD_POOL_SIZE = 1;

    /** singleton instance. */
    private static DirtyManager instance;

    private transient ThreadPoolExecutor executor;

    private final AtomicBoolean isAlive = new AtomicBoolean(true);

    private DirtyDataCollector consumer;

    public DirtyManager(DirtyConf dirtyConf) {
        this.consumer = DataSyncFactoryUtil.discoverDirty(dirtyConf);
    }

    public static DirtyManager fromConfMap(Map confMap) {
        DirtyConf dirtyConf = DirtyConfUtil.parseFromMap(confMap);
        return fromDirtyConf(dirtyConf);
    }

    public static DirtyManager fromDirtyConf(DirtyConf dirtyConf) {
        return new DirtyManager(dirtyConf);
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
                            new FlinkxThreadFactory(
                                    "dirty-consumer",
                                    true,
                                    (t, e) -> {
                                        LOG.error(
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

    public void collect(String data, Throwable cause, String field, RuntimeContext runtimeContext) {
        if (executor == null) {
            execute();
        }

        DirtyDataEntry entity = new DirtyDataEntry();

        Map<String, String> allVariables = runtimeContext.getMetricGroup().getAllVariables();

        entity.setJobId(allVariables.get(JOB_ID));
        entity.setJobName(allVariables.getOrDefault(JOB_NAME, "defaultJobName"));
        entity.setOperatorName(allVariables.getOrDefault(OPERATOR_NAME, "defaultOperatorName"));
        entity.setCreateTime(new Timestamp(System.currentTimeMillis()));
        entity.setDirtyContent(data);
        entity.setFieldName(field);
        entity.setErrorMessage(ExceptionUtil.getErrorMessage(cause));

        consumer.offer(entity);
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
}
