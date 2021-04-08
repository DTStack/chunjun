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

package com.dtstack.flinkx.connectors.stream.outputFormat;

import com.dtstack.flinkx.connectors.stream.conf.StreamConf;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.RowUtil;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * OutputFormat for stream writer
 *
 * @author jiangbo
 * @Company: www.dtstack.com
 */
public class StreamOutputFormat extends BaseRichOutputFormat {

    private StreamConf streamConf;
    protected RowData lastRow;

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        // do nothing
    }

    @Override
    public void writeRecord(RowData rowData) {
        writeSingleRecordInternal(rowData);
    }

    @Override
    protected void writeSingleRecordInternal(RowData row) {
        if (streamConf.isPrint()) {
            LOG.info("subTaskIndex[{}]:{}", taskNumber, RowUtil.rowToStringWithDelimiter(row, streamConf.getWriteDelimiter()));
        }
        lastRow = row;
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        for (RowData row : rows) {
            writeSingleRecordInternal(row);
        }
        if(rows.size() > 1){
            lastRow = rows.get(rows.size() - 1);
        }
    }

    @Override
    public FormatState getFormatState(){
        if(lastRow != null){
            LOG.info("subTaskIndex[{}]:{}", taskNumber, RowUtil.rowToStringWithDelimiter(lastRow, streamConf.getWriteDelimiter()));
        }
        return super.getFormatState();
    }

    public StreamConf getStreamConf() {
        return streamConf;
    }

    public void setStreamConf(StreamConf streamConf) {
        this.streamConf = streamConf;
    }
}
