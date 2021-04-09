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

import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.stream.conf.StreamConf;
import com.dtstack.flinkx.connector.stream.conf.StreamSinkConf;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.RowUtil;

import java.io.IOException;

/**
 * OutputFormat for stream writer
 *
 * @author jiangbo
 * @Company: www.dtstack.com
 *         具体的跟外部系统的交互逻辑
 */
public class StreamOutputFormat extends BaseRichOutputFormat {

    // sinkconf属性
    private StreamSinkConf streamSinkConf;
    // 该类内部自己的需要的变量
    private RowData lastRow;

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        streamSinkConf.getWriter().open(taskNumber, numTasks);
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
        Object data = streamSinkConf.getConverter().toExternal(value);
        LOG.info(rowKind + "(" + data + ")");
        // streamSinkConf.getWriter().write(rowKind + "(" + data + ")");
        if (streamSinkConf.getPrint()) {
            LOG.info(
                    "subTaskIndex[{}]:{}",
                    taskNumber,
                    RowUtil.rowToStringWithDelimiter(value, streamSinkConf.getWriteDelimiter()));
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
                    RowUtil.rowToStringWithDelimiter(lastRow, streamSinkConf.getWriteDelimiter()));
        }
        return super.getFormatState();
    }

    public void setStreamConf(StreamConf streamConf) {
        this.streamSinkConf = (StreamSinkConf) streamConf;
    }
}
