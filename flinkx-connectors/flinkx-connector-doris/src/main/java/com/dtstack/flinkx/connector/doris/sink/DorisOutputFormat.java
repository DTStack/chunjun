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

package com.dtstack.flinkx.connector.doris.sink;

import com.dtstack.flinkx.connector.doris.options.DorisConf;
import com.dtstack.flinkx.connector.doris.rest.DorisLoadClient;
import com.dtstack.flinkx.connector.doris.rest.DorisStreamLoad;
import com.dtstack.flinkx.connector.doris.rest.FeRestService;
import com.dtstack.flinkx.factory.FlinkxThreadFactory;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.ExceptionUtil;

import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * use DorisStreamLoad to write data into doris
 *
 * @author tiezhu@dtstack
 * @date 2021/9/16 星期四
 */
public class DorisOutputFormat extends BaseRichOutputFormat {
    private transient volatile WriteRecordException we = null;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private long flushIntervalMills;
    private DorisConf options;
    private DorisLoadClient client;

    public void setOptions(DorisConf options) {
        this.options = options;
    }

    private String getBackend() throws IOException {
        try {
            // get be url from fe
            return FeRestService.randomBackend(options);
        } catch (IOException e) {
            LOG.error("get backends info fail");
            throw new IOException(e);
        }
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad(options);
        client = new DorisLoadClient(dorisStreamLoad, options, getBackend());
        flushIntervalMills = options.getFlushIntervalMills();
        super.open(taskNumber, numTasks);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        LOG.info("task number : {} , number task : {}", taskNumber, numTasks);
        if (options.getBatchSize() > 1) {
            this.scheduler =
                    new ScheduledThreadPoolExecutor(
                            1, new FlinkxThreadFactory("doris-timer-data-write-thread"));
            this.scheduledFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (DorisOutputFormat.this) {
                                    if (closed) {
                                        return;
                                    }
                                    try {
                                        client.loadCachedCarrier();
                                    } catch (WriteRecordException e) {
                                        LOG.error(
                                                "Writing records failed. {}",
                                                ExceptionUtil.getErrorMessage(e));
                                        we = e;
                                    }
                                }
                            },
                            flushIntervalMills,
                            flushIntervalMills,
                            TimeUnit.MILLISECONDS);
        }
    }

    @Override
    protected void closeInternal() throws IOException {
        // load cache data before close.
        try {
            client.loadCachedCarrier();
        } catch (WriteRecordException e) {
            dirtyManager.collect(e.getRowData(), e, null, getRuntimeContext());
        }
        if (this.scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        checkTimeDataWriteException();
        client.load(rowData, true);
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        checkTimeDataWriteException();
        for (RowData rowData : rows) {
            client.load(rowData, false);
        }
    }

    /** Check for exceptions thrown by timed threads. */
    private void checkTimeDataWriteException() {
        if (we != null) {
            dirtyManager.collect(we.getRowData(), we, null, getRuntimeContext());
            we = null;
        }
    }
}
