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

package com.dtstack.chunjun.connector.doris.sink;

import com.dtstack.chunjun.connector.doris.buffer.BufferFlusher;
import com.dtstack.chunjun.connector.doris.buffer.BufferPools;
import com.dtstack.chunjun.connector.doris.buffer.IBufferPool;
import com.dtstack.chunjun.connector.doris.buffer.RetryableStreamLoadWriter;
import com.dtstack.chunjun.connector.doris.buffer.StreamLoadWriter;
import com.dtstack.chunjun.connector.doris.options.DorisConfig;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.NoRestartException;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.table.data.RowData;

import com.alibaba.fastjson.JSON;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_REQUEST_RETRIES_DEFAULT;

/** use DorisStreamLoad to write data into doris */
@Slf4j
public class DorisHttpOutputFormat extends BaseRichOutputFormat {
    private static final long serialVersionUID = 992571748616683426L;
    @Setter private DorisConfig dorisConf;

    private IBufferPool pools;
    private BufferFlusher writer;
    private volatile AtomicReference<Throwable> flushException;

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        this.flushException = new AtomicReference<>();

        this.writer = new RetryableStreamLoadWriter(new StreamLoadWriter(dorisConf), getRetrys());
        this.pools =
                new BufferPools(
                        dorisConf.getPoolSize(),
                        dorisConf.getMemorySizePerPool() * 1024 * 1024,
                        writer,
                        throwable -> flushException.set(throwable),
                        dorisConf.isKeepOrder(),
                        new Consumer<Long>() {
                            @Override
                            public synchronized void accept(Long num) {
                                numWriteCounter.add(num);
                            }
                        },
                        new Consumer<Long>() {
                            @Override
                            public synchronized void accept(Long size) {
                                bytesWriteCounter.add(size);
                            }
                        });
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        log.info("task number : {} , number task : {}", taskNumber, numTasks);
    }

    @Override
    protected void closeInternal() {
        try {
            pools.shutdown();
            pools = null;
            writer.close();
            writer = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        checkFlushException();
    }

    // 重写此方法，否则numWriteCounter bytesWriteCounter指标重复计算
    @Override
    protected void writeSingleRecord(RowData rowData, LongCounter numWriteCounter) {
        writeSingleRecordInternal(rowData);
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) {
        checkFlushException();
        try {
            Object map = rowConverter.toExternal(rowData, new HashMap<>(columnNameList.size()));
            String json = JSON.toJSONString(map);
            pools.write(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        throw new RuntimeException(
                "DorisHttpOutputFormat does not support writeMultipleRecordsInternal");
    }

    @Override
    protected synchronized void writeRecordInternal() {}

    private int getRetrys() {

        if (dorisConf.getLoadConfig().getRequestRetries() == null) {
            return DORIS_REQUEST_RETRIES_DEFAULT;
        } else {
            return dorisConf.getLoadConfig().getRequestRetries();
        }
    }

    private void checkFlushException() {
        Throwable throwable = flushException.get();
        if (throwable != null) {
            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            for (StackTraceElement stackTraceElement : stack) {
                log.info(
                        stackTraceElement.getClassName()
                                + "."
                                + stackTraceElement.getMethodName()
                                + " line:"
                                + stackTraceElement.getLineNumber());
            }
            throw new NoRestartException("Writing records to Doris failed.", throwable);
        }
    }
}
