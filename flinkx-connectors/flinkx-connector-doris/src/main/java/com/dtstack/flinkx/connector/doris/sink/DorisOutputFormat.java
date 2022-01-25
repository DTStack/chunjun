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
import com.dtstack.flinkx.connector.doris.rest.Carrier;
import com.dtstack.flinkx.connector.doris.rest.DorisLoadClient;
import com.dtstack.flinkx.connector.doris.rest.DorisStreamLoad;
import com.dtstack.flinkx.connector.doris.rest.FeRestService;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * use DorisStreamLoad to write data into doris
 *
 * @author tiezhu@dtstack
 * @date 2021/9/16 星期四
 */
public class DorisOutputFormat extends BaseRichOutputFormat {
    private DorisConf options;
    private DorisLoadClient client;
    /** cache carriers * */
    private final Map<String, Carrier> carrierMap = new HashMap<>();

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
        super.open(taskNumber, numTasks);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        LOG.info("task number : {} , number task : {}", taskNumber, numTasks);
    }

    @Override
    protected void closeInternal() throws IOException {}

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        Carrier carrier = client.process(rowData);
        client.flush(carrier);
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        int size = rows.size();
        for (int i = 0; i < size; i++) {
            client.processBatch(rows.get(i), i, carrierMap);
        }
        if (!carrierMap.isEmpty()) {
            Set<String> keys = carrierMap.keySet();
            for (String key : keys) {
                try {
                    Carrier carrier = carrierMap.get(key);
                    client.flush(carrier);
                    Set<Integer> indexes = carrier.getRowDataIndexes();
                    List<RowData> removeList = new ArrayList<>(indexes.size());
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
}
