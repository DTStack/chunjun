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

package com.dtstack.chunjun.connector.starrocks.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.starrocks.conf.StarRocksConf;
import com.dtstack.chunjun.connector.starrocks.streamload.StarRocksSinkBufferEntity;
import com.dtstack.chunjun.connector.starrocks.streamload.StarRocksStreamLoadFailedException;
import com.dtstack.chunjun.connector.starrocks.streamload.StreamLoadManager;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.table.data.RowData;

import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** @author liuliu 2022/7/12 */
public class StarRocksOutputFormat extends BaseRichOutputFormat {

    StreamLoadManager streamLoadManager;
    private StarRocksConf starRocksConf;
    private StarRocksWriteProcessor writeProcessor;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        List<String> columnNameList =
                starRocksConf.getColumn().stream()
                        .map(FieldConf::getName)
                        .collect(Collectors.toList());

        streamLoadManager = new StreamLoadManager(starRocksConf);
        streamLoadManager.startAsyncFlushing();
        if (starRocksConf.isNameMapped()) {
            writeProcessor = new MappedWriteProcessor(streamLoadManager, starRocksConf);
        } else {
            writeProcessor =
                    new NormalWriteProcessor(
                            rowConverter, streamLoadManager, starRocksConf, columnNameList);
        }
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) {
        // do nothing
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        writeProcessor.write(rows);
        if (rows.size() != batchSize) {
            streamLoadManager.flush(null, false);
        }
    }

    @Override
    public synchronized void writeRecord(RowData rowData) {
        checkTimerWriteException();
        int size = 0;
        rows.add(rowData);
        if (rows.size() >= batchSize) {
            writeRecordInternal();
            size = batchSize;
        }

        updateDuration();
        bytesWriteCounter.add(rowSizeCalculator.getObjectSize(rowData));
        if (checkpointEnabled) {
            snapshotWriteCounter.add(size);
        }
    }

    @Override
    protected synchronized void writeRecordInternal() {
        if (flushEnable.get()) {
            try {
                writeMultipleRecordsInternal();
                numWriteCounter.add(rows.size());
            } catch (Exception e) {
                if (e instanceof StarRocksStreamLoadFailedException) {
                    StarRocksStreamLoadFailedException exception =
                            (StarRocksStreamLoadFailedException) e;
                    String errMessage = handleErrMessage(exception);
                    StarRocksSinkBufferEntity entity = exception.getEntity();
                    for (byte[] data : entity.getBuffer()) {
                        dirtyManager.collect(new String(data), new Throwable(errMessage), null);
                    }
                } else {
                    throw new ChunJunRuntimeException("write starRocks failed.", e);
                }
            } finally {
                // Data is either recorded dirty data or written normally
                rows.clear();
            }
        }
    }

    public String handleErrMessage(StarRocksStreamLoadFailedException e) {
        String message = e.getMessage();
        Map<String, Object> failedResponse = e.getFailedResponse();
        return String.format(
                "write to starRocks failed.\n errMsg:%s\n failedResponse:%s",
                message, JSON.toJSONString(failedResponse));
    }

    @Override
    protected void closeInternal() {
        if (streamLoadManager != null) {
            streamLoadManager.close();
        }
    }

    public void setStarRocksConf(StarRocksConf starRocksConf) {
        this.starRocksConf = starRocksConf;
    }

    public StarRocksConf getStarRocksConf() {
        return starRocksConf;
    }
}
