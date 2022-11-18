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

package com.dtstack.chunjun.cdc;

import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.cdc.handler.CacheHandler;
import com.dtstack.chunjun.cdc.handler.DDLHandler;
import com.dtstack.chunjun.cdc.worker.WorkerManager;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * 数据（不论ddl还是dml数据）下发到对应表名下的unblock队列中，worker在轮询过程中，处理unblock数据队列中的数据，在遇到ddl数据之后，将数据队列置为block状态，并将队
 * 列引用交给store处理，store在拿到队列引用之后，将队列头部的ddl数据下发到外部存储中, 并监听外部存储对ddl的反馈情况（监听工作由store中额外的线程来执行），
 * 此时，队列仍然处于block状态；在收到外部存储的反馈之后，将数据队列头部的ddl数据移除，同时将队列状 态回归为unblock状态，队列引用还给worker。
 */
public class RestorationFlatMap extends RichFlatMapFunction<RowData, RowData> {

    private static final long serialVersionUID = -1936334572949200754L;

    private final QueuesChamberlain chamberlain;
    private final WorkerManager workerManager;

    public RestorationFlatMap(DDLHandler ddlHandler, CacheHandler cacheHandler, CdcConfig conf) {
        this.chamberlain = new QueuesChamberlain(ddlHandler, cacheHandler);
        this.workerManager = new WorkerManager(chamberlain, conf);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        workerManager.open();
        chamberlain.open();
    }

    @Override
    public void close() throws Exception {
        workerManager.close();
        chamberlain.close();
    }

    @Override
    public void flatMap(RowData value, Collector<RowData> out) {
        if (workerManager.getCollector() == null) {
            WrapCollector<RowData> wrapCollector = new WrapCollector<>(out);
            chamberlain.setCollector(wrapCollector);
            workerManager.setCollector(out);
        }

        if (!workerManager.isAlive()) {
            throw new ChunJunRuntimeException(workerManager.getException());
        }
        put(value);
    }

    private void put(RowData rowData) {
        TableIdentifier tableIdentifier = getTableIdentifierFromColumnData(rowData);
        chamberlain.add(rowData, tableIdentifier);
    }

    /**
     * 从 RowData 获取对应的tableIdentifier
     *
     * @param data column row data.
     * @return table identifier.
     */
    private TableIdentifier getTableIdentifierFromColumnData(RowData data) {

        String[] headers;

        if (data instanceof DdlRowData) {
            headers = ((DdlRowData) data).getHeaders();
        } else {
            headers = ((ColumnRowData) data).getHeaders();
        }

        int databaseIndex = -1;
        int schemaIndex = -1;
        int tableIndex = -1;
        for (int i = 0; i < Objects.requireNonNull(headers).length; i++) {
            if ("database".equalsIgnoreCase(headers[i])) {
                databaseIndex = i;
                continue;
            }
            if ("schema".equalsIgnoreCase(headers[i])) {
                schemaIndex = i;
                continue;
            }
            if ("table".equalsIgnoreCase(headers[i])) {
                tableIndex = i;
            }
        }

        String database = getDataFromIndex(databaseIndex, data);
        String schema = getDataFromIndex(schemaIndex, data);
        String table = getDataFromIndex(tableIndex, data);
        return new TableIdentifier(database, schema, table);
    }

    private String getDataFromIndex(int index, RowData data) {
        if (index >= 0) {
            StringData stringData = data.getString(index);
            if (null != stringData) {
                return stringData.toString();
            }
        }
        return null;
    }
}
