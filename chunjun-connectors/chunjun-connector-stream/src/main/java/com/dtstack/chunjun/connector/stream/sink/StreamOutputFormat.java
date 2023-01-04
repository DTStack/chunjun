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

package com.dtstack.chunjun.connector.stream.sink;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.stream.config.StreamConfig;
import com.dtstack.chunjun.connector.stream.util.TablePrintUtil;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/** OutputFormat for stream writer */
public class StreamOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = 341020012101206838L;

    private StreamConfig streamConfig;

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        // do nothing
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            RowData row =
                    (RowData)
                            rowConverter.toExternal(
                                    rowData,
                                    new GenericRowData(rowData.getRowKind(), rowData.getArity()));
            row.setRowKind(rowData.getRowKind());
            if (streamConfig.isPrint()) {
                TablePrintUtil.printTable(row, getFieldNames(rowData));
            }
        } catch (Exception e) {
            throw new WriteRecordException("", e, 0, rowData);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        for (RowData row : rows) {
            writeSingleRecordInternal(row);
        }
    }

    public String[] getFieldNames(RowData rowData) {
        String[] fieldNames = null;
        if (rowData instanceof ColumnRowData) {
            fieldNames = ((ColumnRowData) rowData).getHeaders();
        }

        if (rowData instanceof DdlRowData) {
            fieldNames = ((DdlRowData) rowData).getHeaders();
        }

        if (fieldNames == null) {
            List<FieldConfig> fieldConfigList = streamConfig.getColumn();
            if (CollectionUtils.isNotEmpty(fieldConfigList)) {
                fieldNames =
                        fieldConfigList.stream().map(FieldConfig::getName).toArray(String[]::new);
            }
        }
        return fieldNames;
    }

    @Override
    protected void closeInternal() {
        // do nothing
    }

    public void setStreamConfig(StreamConfig streamConfig) {
        this.streamConfig = streamConfig;
    }
}
