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
import com.dtstack.chunjun.connector.starrocks.connection.StarRocksJdbcConnectionProvider;
import com.dtstack.chunjun.connector.starrocks.options.ConstantValue;
import com.dtstack.chunjun.connector.starrocks.streamload.StarRocksQueryVisitor;
import com.dtstack.chunjun.connector.starrocks.streamload.StarRocksSinkBufferEntity;
import com.dtstack.chunjun.connector.starrocks.streamload.StarRocksStreamLoadFailedException;
import com.dtstack.chunjun.connector.starrocks.streamload.StreamLoadManager;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.table.data.RowData;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class StarRocksOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = -72510119599895395L;

    StreamLoadManager streamLoadManager;
    private StarRocksConfig starRocksConfig;
    private StarRocksWriteProcessor writeProcessor;

    @Override
    public void initializeGlobal(int parallelism) {
        executeBatch(starRocksConfig.getPreSql());
    }

    @Override
    public void finalizeGlobal(int parallelism) {
        executeBatch(starRocksConfig.getPostSql());
    }

    /**
     * 执行pre、post SQL
     *
     * @param sqlList
     */
    protected void executeBatch(List<String> sqlList) {
        if (CollectionUtils.isNotEmpty(sqlList)) {
            StarRocksQueryVisitor starrocksQueryVisitor =
                    new StarRocksQueryVisitor(starRocksConfig);
            StarRocksJdbcConnectionProvider jdbcConnProvider =
                    starrocksQueryVisitor.getJdbcConnProvider();
            try {
                jdbcConnProvider.checkValid();
                Statement stmt = jdbcConnProvider.getConnection().createStatement();
                for (String sql : sqlList) {
                    // 兼容多条SQL写在同一行的情况
                    String[] strings = sql.split(";");
                    for (String s : strings) {
                        if (StringUtils.isNotBlank(s)) {
                            log.info("add sql to batch, sql = {}", s);
                            stmt.addBatch(s);
                        }
                    }
                }
                stmt.executeBatch();
            } catch (ClassNotFoundException se) {
                throw new IllegalArgumentException(
                        "Failed to find jdbc driver." + se.getMessage(), se);
            } catch (SQLException e) {
                throw new RuntimeException(
                        "execute sql failed, sqlList = " + GsonUtil.GSON.toJson(sqlList), e);
            } finally {
                jdbcConnProvider.close();
            }
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        List<String> columnNameList =
                starRocksConfig.getColumn().stream()
                        .map(FieldConfig::getName)
                        .collect(Collectors.toList());

        streamLoadManager = new StreamLoadManager(starRocksConfig);
        if (!streamLoadManager.tableHasPartition()) {
            throw new RuntimeException(
                    "data cannot be inserted into table with empty partition. "
                            + "Use `SHOW PARTITIONS FROM "
                            + starRocksConfig.getTable()
                            + "` to see the currently partitions of this table");
        }
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
        if (rowData instanceof ColumnRowData) {
            batchMaxByteSize += ((ColumnRowData) rowData).getByteSize();
        } else {
            batchMaxByteSize += rowSizeCalculator.getObjectSize(rowData);
        }
        if (rows.size() >= starRocksConfig.getBatchSize()
                || batchMaxByteSize >= ConstantValue.SINK_BATCH_MAX_BYTES_DEFAULT) {
            writeRecordInternal();
            size = batchSize;
        }

        updateDuration();
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
                bytesWriteCounter.add(batchMaxByteSize);
            } catch (Exception e) {
                if (e instanceof StarRocksStreamLoadFailedException) {
                    StarRocksStreamLoadFailedException exception =
                            (StarRocksStreamLoadFailedException) e;
                    String errMessage = handleErrMessage(exception);
                    StarRocksSinkBufferEntity entity = exception.getEntity();
                    for (byte[] data : entity.getBuffer()) {
                        long globalErrors =
                                accumulatorCollector.getAccumulatorValue(Metrics.NUM_ERRORS, false);
                        dirtyManager.collect(
                                new String(data), new Throwable(errMessage), null, globalErrors);
                    }
                } else {
                    throw new ChunJunRuntimeException("write starRocks failed.", e);
                }
            } finally {
                // Data is either recorded dirty data or written normally
                rows.clear();
                batchMaxByteSize = 0;
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
    public synchronized void close() throws IOException {
        super.close();
        // 解决当异步执行streamLoad时，flushException不为空，则认为整个任务应该抛出异常
        if (streamLoadManager != null && streamLoadManager.getFlushException() != null) {
            throw new RuntimeException(streamLoadManager.getFlushException());
        }
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
