/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.connector.kudu.sink;

import com.dtstack.flinkx.connector.kudu.conf.KuduSinkConf;
import com.dtstack.flinkx.connector.kudu.util.KuduUtil;
import com.dtstack.flinkx.sink.WriteMode;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.NoRestartException;
import com.dtstack.flinkx.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class KuduOutputFormat extends BaseRichOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);

    private static final long serialVersionUID = 1L;

    private KuduSinkConf sinkConf;

    private KuduClient client;

    private KuduSession session;

    private KuduTable kuduTable;

    private WriteMode writeMode;

    private SessionConfiguration.FlushMode flushMode;

    @Override
    @SuppressWarnings("unchecked")
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            Operation operation = toOperation(writeMode);
            rowConverter.toExternal(rowData, operation);
            // 当flush-mode 为 auto_flush_sync 时，session apply 就会将数据写入，同时将response 返回
            applyOperation(operation);
            session.flush().forEach(this::dealResponse);
        } catch (Exception e) {
            throw new WriteRecordException(
                    "Kudu output-format writeSingleRecordInternal failed. ", e, 0, rowData);
        }
    }

    /**
     * Deal response when operation apply. At MANUAL_FLUSH mode, response returns after {@link
     * KuduSession#flush()}. But at AUTO_FLUSH_SYNC mode, response returns after {@link
     * KuduSession#apply(Operation)}
     *
     * @param response {@link OperationResponse} response after operation done.
     */
    private void dealResponse(OperationResponse response) {
        if (response.hasRowError()) {
            RowError error = response.getRowError();
            String errorMsg = error.getErrorStatus().toString();

            if (error.getErrorStatus().isNotFound()
                    || error.getErrorStatus().isIOError()
                    || error.getErrorStatus().isRuntimeError()
                    || error.getErrorStatus().isServiceUnavailable()
                    || error.getErrorStatus().isIllegalState()) {
                throw new FlinkxRuntimeException(errorMsg);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void writeMultipleRecordsInternal() throws Exception {
        for (RowData rowData : rows) {
            Operation operation = toOperation(writeMode);
            rowConverter.toExternal(rowData, operation);
            applyOperation(operation);
        }
        session.flush().forEach(this::dealResponse);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        writeMode = sinkConf.getWriteMode();
        flushMode = transformFlushMode(sinkConf.getFlushMode());
        try {
            client = KuduUtil.getKuduClient(sinkConf);
        } catch (Exception e) {
            throw new NoRestartException("Get KuduClient error", e);
        }

        session = client.newSession();
        session.setMutationBufferSpace(sinkConf.getMaxBufferSize());
        kuduTable = client.openTable(sinkConf.getTable());

        switch (flushMode.name().toLowerCase(Locale.ENGLISH)) {
            case "auto_flush_background":
                LOG.warn(
                        "Unable to determine the order of data at AUTO_FLUSH_BACKGROUND mode. "
                                + "Only [batchWaitInterval] will effect.");
                session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
                break;
            case "manual_flush":
                session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
                break;
            default:
                LOG.warn("Parameter [batchSize] will not take effect at AUTO_FLUSH_SYNC mode.");
                session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        }
    }

    private Operation toOperation(WriteMode writeMode) {
        switch (writeMode) {
            case INSERT:
                return kuduTable.newInsert();
            case UPDATE:
                return kuduTable.newUpdate();
            default:
                return kuduTable.newUpsert();
        }
    }

    private SessionConfiguration.FlushMode transformFlushMode(String flushMode) {
        switch (flushMode.toUpperCase(Locale.ENGLISH)) {
            case "MANUAL_FLUSH":
                return SessionConfiguration.FlushMode.MANUAL_FLUSH;
            case "AUTO_FLUSH_BACKGROUND":
                return SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND;
            case "AUTO_FLUSH_SYNC":
            default:
                return SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC;
        }
    }

    private void applyOperation(Operation operation) throws KuduException {
        if (flushMode.equals(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC)) {
            dealResponse(session.apply(operation));
        } else {
            session.apply(operation);
        }
    }

    @Override
    protected void closeInternal() throws IOException {

        if (session != null && !session.isClosed()) {
            session.flush();
            session.close();
            session = null;
        }

        if (client != null) {
            client.close();
            client = null;
        }
    }

    public KuduSinkConf getKuduSinkConf() {
        return sinkConf;
    }

    public void setKuduSinkConf(KuduSinkConf sinkConf) {
        this.sinkConf = sinkConf;
    }
}
