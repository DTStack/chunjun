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

package com.dtstack.flinkx.connector.dorisbatch.sink;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.dorisbatch.options.DorisConf;
import com.dtstack.flinkx.connector.dorisbatch.rest.DorisStreamLoad;
import com.dtstack.flinkx.connector.dorisbatch.rest.FeRestService;
import com.dtstack.flinkx.connector.dorisbatch.rest.module.Field;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

/**
 * use DorisStreamLoad to write data into doris
 *
 * @author tiezhu@dtstack
 * @date 2021/9/16 星期四
 */
public class DorisOutputFormat extends BaseRichOutputFormat {

    private List<String> columnNames;

    private DorisConf options;

    private String fieldDelimiter;

    private String lineDelimiter;

    private DorisStreamLoad dorisStreamLoad;

    private void setColumnName(List<FieldConf> columns) {
        this.columnNames = new LinkedList<>();
        if (columns != null) {
            columns.forEach(column -> columnNames.add(column.getName()));
        }
    }

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

    public void flush(String data) throws IOException {
        Integer maxRetries = options.getMaxRetries();
        for (int i = 0; i <= maxRetries; i++) {
            try {
                dorisStreamLoad.load(columnNames, data);
                break;
            } catch (IOException e) {
                LOG.error("doris sink error, retry times = {}", i, e);
                if (i >= maxRetries) {
                    throw new IOException(e);
                }
                try {
                    dorisStreamLoad.setHostPort(getBackend());
                    LOG.warn("streamLoad error,switch be: {}", dorisStreamLoad.getLoadUrlStr(), e);
                    TimeUnit.MILLISECONDS.sleep(1000L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.fieldDelimiter = options.getFieldDelimiter();
        this.lineDelimiter = options.getLineDelimiter();
        dorisStreamLoad = new DorisStreamLoad(getBackend(), options);
        LOG.info("StreamLoad BE:{}", dorisStreamLoad.getLoadUrlStr());
        super.open(taskNumber, numTasks);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        LOG.info("task number : {} , number task : {}", taskNumber, numTasks);
        List<Field> fields = FeRestService.getSchema(options).getProperties();
        List<String> fullColumns = new ArrayList<>();
        for (Field field : fields) {
            fullColumns.add(field.getName());
        }
        setColumnName(options.getColumn());
        // ((DorisColumnConverter) rowConverter).setFullColumn(fullColumns);
        // ((DorisColumnConverter) rowConverter).setColumnNames(columnNames);
    }

    @Override
    protected void closeInternal() throws IOException {}

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        StringJoiner value = new StringJoiner(this.fieldDelimiter);
        try {
            rowConverter.toExternal(rowData, value);
            flush(value.toString());
        } catch (Exception e) {
            String errormessage = recordConvertDetailErrorMessage(-1, rowData);
            LOG.error(errormessage, e);
            throw new WriteRecordException(errormessage, e, -1, rowData);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        List<String> batch = new ArrayList<>();
        try {
            for (RowData rowData : rows) {
                StringJoiner value = new StringJoiner(this.fieldDelimiter);
                rowConverter.toExternal(rowData, value);
                batch.add(value.toString());
            }
            flush(String.join(lineDelimiter, batch));
        } catch (Exception e) {
            LOG.error(
                    "write Multiple Records error, row size = {}, first row = {}",
                    rows.size(),
                    rows.size() > 0 ? GsonUtil.GSON.toJson(rows.get(0)) : "null",
                    e);
            throw e;
        }
    }
}
