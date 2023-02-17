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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.connector.starrocks.streamload.StarRocksSinkBufferEntity;
import com.dtstack.chunjun.connector.starrocks.streamload.StarRocksStreamLoadFailedException;
import com.dtstack.chunjun.connector.starrocks.streamload.StreamLoadManager;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.table.data.RowData;

import com.alibaba.fastjson.JSON;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StarRocksOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = -72510119599895395L;

    StreamLoadManager streamLoadManager;
    private StarRocksConfig starRocksConfig;
    private StarRocksWriteProcessor writeProcessor;

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        List<String> columnNameList =
                starRocksConfig.getColumn().stream()
                        .map(FieldConfig::getName)
                        .collect(Collectors.toList());

        streamLoadManager = new StreamLoadManager(starRocksConfig);
        streamLoadManager.startAsyncFlushing();
        if (starRocksConfig.isNameMapped()) {
            writeProcessor = new MappedWriteProcessor(streamLoadManager, starRocksConfig);
        } else {
            writeProcessor =
                    new NormalWriteProcessor(
                            rowConverter, streamLoadManager, starRocksConfig, columnNameList);
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

    public void setStarRocksConf(StarRocksConfig starRocksConfig) {
        this.starRocksConfig = starRocksConfig;
    }

    public StarRocksConfig getStarRocksConf() {
        return starRocksConfig;
    }
}
