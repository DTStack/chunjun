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

import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.stream.conf.StreamSinkConf;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.RowUtil;

import java.util.Objects;

/**
 * OutputFormat for stream writer
 *
 * @author jiangbo
 * @Company: www.dtstack.com
 *         具体的跟外部系统的交互逻辑
 */
public class StreamOutputFormat extends BaseRichOutputFormat {

    // streamSinkConf属性
    private StreamSinkConf streamSinkConf;
    // 该类内部自己的需要的变量
    private DynamicTableSink.DataStructureConverter converter;
    private RowData lastRow;

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        // do nothing
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) {
        String rowKind = rowData.getRowKind().shortString();
        if (Objects.nonNull(converter)) {
            LOG.info(rowKind + "(" + converter.toExternal(rowData) + ")");
        }
        if (streamSinkConf.getPrint()) {
            LOG.info(
                    "subTaskIndex[{}]:{}",
                    taskNumber,
                    RowUtil.rowToStringWithDelimiter(rowData, streamSinkConf.getWriteDelimiter()));
        }
        lastRow = rowData;
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        for (RowData row : rows) {
            writeSingleRecordInternal(row);
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

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // do nothing
    }

    public void setStreamSinkConf(StreamSinkConf streamSinkConf) {
        this.streamSinkConf = streamSinkConf;
    }

    public void setConverter(DynamicTableSink.DataStructureConverter converter) {
        this.converter = converter;
    }
}
