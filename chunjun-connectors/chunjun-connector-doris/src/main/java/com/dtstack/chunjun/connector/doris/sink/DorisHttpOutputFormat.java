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

import com.dtstack.chunjun.connector.doris.options.DorisConfig;
import com.dtstack.chunjun.connector.doris.rest.Carrier;
import com.dtstack.chunjun.connector.doris.rest.DorisLoadClient;
import com.dtstack.chunjun.connector.doris.rest.DorisStreamLoad;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** use DorisStreamLoad to write data into doris */
@Slf4j
public class DorisHttpOutputFormat extends BaseRichOutputFormat {
    private static final long serialVersionUID = 992571748616683426L;
    private DorisConfig options;
    private DorisLoadClient client;
    /** cache carriers * */
    private final Map<String, Carrier> carrierMap = new HashMap<>();

    private List<String> columns;

    public void setOptions(DorisConfig options) {
        this.options = options;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad(options);
        dorisStreamLoad.replaceBackend();
        client = new DorisLoadClient(dorisStreamLoad, options.isNameMapped(), options);
        super.open(taskNumber, numTasks);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        log.info("task number : {} , number task : {}", taskNumber, numTasks);
    }

    @Override
    protected void closeInternal() {}

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            client.process(rowData, columns, rowConverter);
        } catch (Exception e) {
            throw new WriteRecordException("", e, 0, rowData);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        int size = rows.size();
        for (int i = 0; i < size; i++) {
            client.process(rows.get(i), i, carrierMap, columns, rowConverter);
        }
        if (!carrierMap.isEmpty()) {
            Set<String> keys = carrierMap.keySet();
            for (String key : keys) {
                try {
                    Carrier carrier = carrierMap.get(key);
                    client.flush(carrier);
                    Set<Integer> indexes = carrier.getRowDataIndexes();
                    List<RowData> removeList = new ArrayList<>(indexes.size());
                    // Add the amount of data written successfully.
                    numWriteCounter.add(indexes.size());
                    for (int index : indexes) {
                        removeList.add(rows.get(index));
                    }
                    // Remove RowData from rows after a successful write
                    // to prevent multiple writes.
                    rows.removeAll(removeList);
                } finally {
                    carrierMap.remove(key);
                }
            }
        }
    }

    @Override
    protected synchronized void writeRecordInternal() {
        if (flushEnable.get()) {
            try {
                writeMultipleRecordsInternal();
            } catch (Exception e) {
                // 批量写异常转为单条写
                rows.forEach(item -> writeSingleRecord(item, numWriteCounter));
            } finally {
                // Data is either recorded dirty data or written normally
                rows.clear();
            }
        }
    }
}
