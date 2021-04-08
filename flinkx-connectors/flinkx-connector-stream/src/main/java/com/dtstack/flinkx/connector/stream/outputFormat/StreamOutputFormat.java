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

package com.dtstack.flinkx.connector.stream.outputFormat;

import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.stream.conf.StreamConf;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.RowUtil;

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

    private DynamicTableSink.DataStructureConverter converter;
    private PrintSinkOutputWriter<String> writer;


    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        writer.open(taskNumber, numTasks);
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
    protected void writeSingleRecordInternal(RowData value) {
        String rowKind = value.getRowKind().shortString();
        Object data = converter.toExternal(value);
        writer.write(rowKind + "(" + data + ")");
        if (streamConf.isPrint()) {
            // LOG.info("subTaskIndex[{}]:{}", taskNumber, RowUtil.rowToStringWithDelimiter(value, streamConf.getWriteDelimiter()));
        }
        lastRow = value;
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        for (RowData row : rows) {
            writeSingleRecordInternal(row);
        }
        if (rows.size() > 1) {
            lastRow = rows.get(rows.size() - 1);
        }
    }

    @Override
    public FormatState getFormatState() {
        if (lastRow != null) {
            LOG.info(
                    "subTaskIndex[{}]:{}",
                    taskNumber,
                    RowUtil.rowToStringWithDelimiter(lastRow, streamConf.getWriteDelimiter()));
        }
        return super.getFormatState();
    }

    public StreamConf getStreamConf() {
        return streamConf;
    }

    public void setStreamConf(StreamConf streamConf) {
        this.streamConf = streamConf;
    }

    public DynamicTableSink.DataStructureConverter getConverter() {
        return converter;
    }

    public void setConverter(DynamicTableSink.DataStructureConverter converter) {
        this.converter = converter;
    }

    public PrintSinkOutputWriter<String> getWriter() {
        return writer;
    }

    public void setWriter(PrintSinkOutputWriter<String> writer) {
        this.writer = writer;
    }
}
