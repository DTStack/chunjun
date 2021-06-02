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

package com.dtstack.flinkx.connector.stream.sink;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.stream.conf.StreamConf;
import com.dtstack.flinkx.connector.stream.util.TablePrintUtil;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * OutputFormat for stream writer
 *
 * @author jiangbo @Company: www.dtstack.com 具体的跟外部系统的交互逻辑
 */
public class StreamOutputFormat extends BaseRichOutputFormat {

    // streamSinkConf属性
    private StreamConf streamConf;
    private GenericRowData lastRow;

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        // do nothing
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) {
        try {
            GenericRowData genericRowData = new GenericRowData(rowData.getArity());
            GenericRowData row = (GenericRowData) rowConverter.toExternal(rowData, genericRowData);
            if (streamConf.getPrint()) {
                TablePrintUtil.printTable(row, getFieldNames(rowData));
            }
            lastRow = row;
        } catch (Exception e) {
            LOG.error("row = {}, e = {}", rowData, ExceptionUtil.getErrorMessage(e));
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        for (RowData row : rows) {
            writeSingleRecordInternal(row);
        }
    }

    @Override
    protected void preCommit() {
        if (lastRow != null) {
            TablePrintUtil.printTable(lastRow, getFieldNames(lastRow));
        }
    }

    public String[] getFieldNames(RowData rowData) {
        String[] fieldNames = null;
        if(rowData instanceof ColumnRowData){
            fieldNames = ((ColumnRowData) rowData).getHeaders();
        }
        if(fieldNames == null){
            List<FieldConf> fieldConfList = streamConf.getColumn();
            if (CollectionUtils.isNotEmpty(fieldConfList)) {
                fieldNames = fieldConfList.stream().map(FieldConf::getName).toArray(String[]::new);
            }
        }
        return fieldNames;
    }

    @Override
    protected void closeInternal() {
        // do nothing
    }

    @Override
    protected void commit(long checkpointId) {

    }

    @Override
    protected void rollback(long checkpointId) {

    }

    public void setStreamConf(StreamConf streamConf) {
        this.streamConf = streamConf;
    }
}
