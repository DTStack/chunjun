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

package com.dtstack.chunjun.connector.nebula.sink;

import com.dtstack.chunjun.connector.nebula.client.NebulaClientFactory;
import com.dtstack.chunjun.connector.nebula.client.NebulaSession;
import com.dtstack.chunjun.connector.nebula.client.NebulaStorageClient;
import com.dtstack.chunjun.connector.nebula.config.NebulaConfig;
import com.dtstack.chunjun.connector.nebula.row.NebulaRows;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import com.google.common.collect.Lists;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class NebulaOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = 3745600530397638831L;

    private NebulaConfig nebulaConfig;

    private NebulaSession session;
    private NebulaStorageClient storageClient;

    @Override
    public void initializeGlobal(int parallelism) {
        session = NebulaClientFactory.createNebulaSession(nebulaConfig);
        storageClient = NebulaClientFactory.createNebulaStorageClient(nebulaConfig);
        try {
            session.init();
            storageClient.init();
            check();
            closeInternal();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ChunJunRuntimeException(e);
        }
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            ArrayList<RowData> var = Lists.newArrayList(rowData);
            flush(var);
        } catch (Exception e) {
            throw new WriteRecordException("", e, 0, rowData);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        flush(rows);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        session = NebulaClientFactory.createNebulaSession(nebulaConfig);
        storageClient = NebulaClientFactory.createNebulaStorageClient(nebulaConfig);
        try {
            session.init();
            storageClient.init();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    protected void closeInternal() {
        if (session != null) {
            session.close();
        }
        if (storageClient != null) {
            storageClient.close();
        }
    }

    /**
     * write data to nebula
     *
     * @param rows rows which should write to nebula
     * @throws Exception
     */
    private void flush(List<RowData> rows) throws Exception {
        String statements;
        NebulaRows nebulaRows = new NebulaRows(nebulaConfig);
        for (RowData row : rows) {
            nebulaRows = (NebulaRows) rowConverter.toExternal(row, nebulaRows);
        }
        String useSpace = String.format("use %s;", nebulaConfig.getSpace());
        switch (nebulaConfig.getMode()) {
            case UPSERT:
                statements = useSpace + nebulaRows.getUpsertStatement();
                break;
            case INSERT:
                statements = useSpace + nebulaRows.getInsertStatement();
                break;
            default:
                throw new ChunJunRuntimeException("Unsupported write type of nebula sink!");
        }
        try {
            ResultSet res = session.execute(statements);
            if (res.getErrorCode() != 0) {}

            if (!res.isSucceeded()) {
                log.warn("write failed!");
                for (RowData row : rows) {
                    WriteRecordException exception =
                            new WriteRecordException(
                                    res.getErrorMessage(),
                                    new ChunJunRuntimeException(res.getErrorMessage()));
                    long globalErrors =
                            accumulatorCollector.getAccumulatorValue(Metrics.NUM_ERRORS, false);
                    dirtyManager.collect(row, exception, null, globalErrors);
                }
                if (errCounter != null) {
                    errCounter.add(rows.size());
                }
            }
        } catch (Exception e) {
            log.error("write failed!");
            WriteRecordException exception;
            for (RowData row : rows) {
                exception = new WriteRecordException(e.getMessage(), e);
                long globalErrors =
                        accumulatorCollector.getAccumulatorValue(Metrics.NUM_ERRORS, false);
                dirtyManager.collect(row, exception, null, globalErrors);
            }

            if (errCounter != null) {
                errCounter.add(rows.size());
            }
        }
    }

    public NebulaConfig getNebulaConf() {
        return nebulaConfig;
    }

    public void setNebulaConf(NebulaConfig nebulaConfig) {
        this.nebulaConfig = nebulaConfig;
    }

    /**
     * check the space and tag/edge exist,if not exist,create
     *
     * @throws IOErrorException
     */
    private void check() throws IOErrorException, InterruptedException {
        Boolean spaceExist = storageClient.isSpaceExist(nebulaConfig.getSpace());
        if (!spaceExist) {
            log.info("space dose not exist,create space");
            ResultSet res = session.createSpace(nebulaConfig.getSpace());
            if (!res.isSucceeded()) {
                throw new ChunJunRuntimeException("create space failed: " + res.getErrorMessage());
            }
            // ensure the created space has synchronized in nebula cluster
            Thread.sleep(1000 * 5L);
        }
        Boolean schemaExist =
                storageClient.isSchemaExist(nebulaConfig.getSpace(), nebulaConfig.getEntityName());
        if (!schemaExist) {
            log.info("tag/edge dose not exist,create tag/edge");
            ResultSet res =
                    session.createSchema(nebulaConfig.getSpace(), nebulaConfig.getEntityName());
            if (!res.isSucceeded()) {
                throw new ChunJunRuntimeException(
                        "create tag/edge failed: " + res.getErrorMessage());
            }
            // ensure the created tag/edge has synchronized in nebula cluster
            Thread.sleep(1000 * 5L);
        }
    }
}
