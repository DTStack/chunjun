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
import com.dtstack.chunjun.connector.selectdbcloud.rest.SelectDBCloudStageLoad;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.selectdbcloud.common.LoadConstants.*;
import static org.apache.flink.table.data.RowData.createFieldGetter;

public class SeletdbcloudWriter {

    public static final Logger LOG = LoggerFactory.getLogger(SeletdbcloudWriter.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String[] fieldNames;

    private boolean jsonFormat;

    private String loadDataStr = null;

    private RowData.FieldGetter[] fieldGetters;

    private String fieldDelimiter;

    private String lineDelimiter;

    private final SelectdbcloudConfig conf;

    private final SelectDBCloudStageLoad stageLoad;

    public SeletdbcloudWriter(SelectdbcloudConfig conf) {
        this.conf = conf;
        this.stageLoad = new SelectDBCloudStageLoad(conf);
        init();
    }

    public void init() {
        this.jsonFormat = JSON.equals(conf.getLoadProperties().getProperty(FORMAT_KEY));
        this.fieldNames = conf.getFieldNames();
        handleStreamloadProp();
        LogicalType[] logicalTypes =
                Arrays.stream(conf.getFieldDataTypes())
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new);
        this.fieldGetters = new RowData.FieldGetter[logicalTypes.length];
        for (int i = 0; i < logicalTypes.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[i], i);
        }
    }

    private void handleStreamloadProp() {
        Properties streamLoadProp = conf.getLoadProperties();
        boolean ifEscape =
                Boolean.parseBoolean(
                        streamLoadProp.getProperty(
                                ESCAPE_DELIMITERS_KEY, ESCAPE_DELIMITERS_DEFAULT));
        if (ifEscape) {
            this.fieldDelimiter =
                    escapeString(
                            streamLoadProp.getProperty(
                                    FIELD_DELIMITER_KEY, FIELD_DELIMITER_DEFAULT));
            this.lineDelimiter =
                    escapeString(
                            streamLoadProp.getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT));

            if (streamLoadProp.contains(ESCAPE_DELIMITERS_KEY)) {
                streamLoadProp.remove(ESCAPE_DELIMITERS_KEY);
            }
        } else {
            this.fieldDelimiter =
                    streamLoadProp.getProperty(FIELD_DELIMITER_KEY, FIELD_DELIMITER_DEFAULT);
            this.lineDelimiter =
                    streamLoadProp.getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT);
        }
    }

    private String escapeString(String s) {
        Pattern p = Pattern.compile("\\\\x(\\d{2})");
        Matcher m = p.matcher(s);

        StringBuffer buf = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(buf, String.format("%s", (char) Integer.parseInt(m.group(1))));
        }
        m.appendTail(buf);
        return buf.toString();
    }

    public void write(List<RowData> rows) throws IOException {
        loadDataStr = processRowData(rows);
        flush();
    }

    private String parseDeleteSign(RowKind rowKind) {
        if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
            return "0";
        } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
            return "1";
        } else {
            throw new RuntimeException("Unrecognized row kind:" + rowKind.toString());
        }
    }

    private synchronized void flush() throws IOException {
        int maxRetries = conf.getMaxRetries();
        for (int i = 0; i <= maxRetries; i++) {
            try {
                stageLoad.load(loadDataStr);
                loadDataStr = null;
                break;
            } catch (Exception e) {
                LOG.error("selectdb sink error, retry times = {}", i, e);
                if (i >= maxRetries) {
                    throw new IOException(e);
                }
                try {
                    LOG.warn("load error,retry: {}", stageLoad.getLoadUrlStr(), e);
                    Thread.sleep(1000L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    private String processRowData(List<RowData> rows) throws JsonProcessingException {

        if (jsonFormat) {
            return processJsonFormat(rows);
        } else {
            return processStringFormat(rows);
        }
    }

    private String processJsonFormat(List<RowData> rows) throws JsonProcessingException {

        List<Map<String, String>> mapData = new ArrayList<>(rows.size());

        for (RowData rowData : rows) {
            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < rowData.getArity() && i < fieldGetters.length; ++i) {
                Object field = fieldGetters[i].getFieldOrNull(rowData);
                String data = field != null ? field.toString() : null;
                map.put(this.fieldNames[i], data);
            }
            if (conf.getEnableDelete()) {
                map.put(DELETE_SIGN, parseDeleteSign(rowData.getRowKind()));
            }
            mapData.add(map);
        }

        return OBJECT_MAPPER.writeValueAsString(mapData);
    }

    private String processStringFormat(List<RowData> rows) {

        List<String> strData = new ArrayList<>(rows.size());

        for (RowData rowData : rows) {

            StringJoiner value = new StringJoiner(this.fieldDelimiter);

            for (int i = 0; i < rowData.getArity() && i < fieldGetters.length; ++i) {
                Object field = fieldGetters[i].getFieldOrNull(rowData);
                String data = field != null ? field.toString() : NULL_VALUE;
                value.add(data);
            }
            // add doris delete sign
            if (conf.getEnableDelete()) {
                value.add(parseDeleteSign(rowData.getRowKind()));
            }

            strData.add(value.toString());
        }

        return strData.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(this.lineDelimiter));
    }

    public synchronized void close() {
        if (stageLoad != null) {
            try {
                stageLoad.close();
            } catch (IOException e) {
                LOG.error("close stage load client failed, err: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }
}
