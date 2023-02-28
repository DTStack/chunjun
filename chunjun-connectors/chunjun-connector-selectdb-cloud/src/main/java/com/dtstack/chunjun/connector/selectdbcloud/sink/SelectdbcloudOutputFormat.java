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

package com.dtstack.chunjun.connector.selectdbcloud.sink;

import com.dtstack.chunjun.connector.selectdbcloud.options.SelectdbcloudConfig;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import java.io.IOException;

public class SelectdbcloudOutputFormat extends BaseRichOutputFormat {

    private SelectdbcloudConfig conf;

    private SeletdbcloudWriter writer;

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        // do nothing
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        if (rows.isEmpty()) {
            return;
        }
        writer.write(rows);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        writer = new SeletdbcloudWriter(conf);
    }

    @Override
    protected void closeInternal() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public synchronized void writeRecord(RowData rowData) {
        checkTimerWriteException();
        int size = 0;
        rows.add(rowData);
        if (rows.size() >= batchSize) {
            writeRecordInternal();
        }
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
        bytesWriteCounter.add(rowSizeCalculator.getObjectSize(rowData));
        if (checkpointEnabled) {
            snapshotWriteCounter.add(size);
        }
    }

    public void setConf(SelectdbcloudConfig conf) {
        this.conf = conf;
    }
}
