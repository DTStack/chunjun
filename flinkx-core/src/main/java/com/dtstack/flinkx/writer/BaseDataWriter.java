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

package com.dtstack.flinkx.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.DirtyConfig;
import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.reader.MetaColumn;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstract specification of Writer Plugin
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class BaseDataWriter {

    protected String mode;

    protected String monitorUrls;

    protected Integer errors;

    protected Double errorRatio;

    protected String dirtyPath;

    protected Map<String, Object> dirtyHadoopConfig;

    protected RestoreConfig restoreConfig;

    protected List<String> srcCols = new ArrayList<>();

    protected static ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public List<String> getSrcCols() {
        return srcCols;
    }

    public void setSrcCols(List<String> srcCols) {
        this.srcCols = srcCols;
    }

    public BaseDataWriter(DataTransferConfig config) {
        this.monitorUrls = config.getMonitorUrls();
        this.restoreConfig = config.getJob().getSetting().getRestoreConfig();
        this.errors = config.getJob().getSetting().getErrorLimit().getRecord();
        Double percentage = config.getJob().getSetting().getErrorLimit().getPercentage();
        if (percentage != null) {
            this.errorRatio = percentage / 100.0;
        }

        DirtyConfig dirtyConfig = config.getJob().getSetting().getDirty();
        if (dirtyConfig != null) {
            String dirtyPath = dirtyConfig.getPath();
            Map<String, Object> dirtyHadoopConfig = dirtyConfig.getHadoopConfig();
            if (dirtyPath != null) {
                this.dirtyPath = dirtyPath;
            }
            if (dirtyHadoopConfig != null) {
                this.dirtyHadoopConfig = dirtyHadoopConfig;
            }
        }

        List columns = config.getJob().getContent().get(0).getReader().getParameter().getColumn();
        parseSrcColumnNames(columns);

        if (restoreConfig.isStream()) {
            return;
        }

        if (restoreConfig.isRestore()) {
            MetaColumn metaColumn = MetaColumn.getMetaColumn(columns, restoreConfig.getRestoreColumnName());
            if (metaColumn == null) {
                throw new RuntimeException("Can not find restore column from json with column name:" + restoreConfig.getRestoreColumnName());
            }
            restoreConfig.setRestoreColumnIndex(metaColumn.getIndex());
            restoreConfig.setRestoreColumnType(metaColumn.getType());
        }
    }

    private void parseSrcColumnNames(List columns) {
        if (columns == null) {
            return;
        }

        if (columns.isEmpty()) {
            throw new RuntimeException("source columns can't be null or empty");
        }

        if (columns.get(0) instanceof String) {
            for (Object column : columns) {
                srcCols.add((String) column);
            }
            return;
        }

        if (columns.get(0) instanceof Map) {
            for (Object column : columns) {
                Map<String, Object> colMap = (Map<String, Object>) column;
                String colName = (String) colMap.get("name");
                if (StringUtils.isBlank(colName)) {
                    Object colIndex = colMap.get("index");
                    if (colIndex != null) {
                        if (colIndex instanceof Integer) {
                            colName = String.valueOf(colIndex);
                        } else if (colIndex instanceof Double) {
                            Double doubleColIndex = (Double) colIndex;
                            colName = String.valueOf(doubleColIndex.intValue());
                        } else {
                            throw new RuntimeException("invalid src col index");
                        }
                    } else {
                        String colValue = (String) colMap.get("value");
                        if (StringUtils.isNotBlank(colValue)) {
                            colName = "val_" + colValue;
                        } else {
                            throw new RuntimeException("can't determine source column name");
                        }
                    }
                }
                srcCols.add(colName);
            }
        }
    }

    /**
     * Build the write data flow with read data flow
     *
     * @param dataSet read data flow
     * @return write data flow
     */
    public abstract DataStreamSink<?> writeData(DataStream<Row> dataSet);

    @SuppressWarnings("unchecked")
    protected DataStreamSink<?> createOutput(DataStream<?> dataSet, OutputFormat outputFormat, String sinkName) {
        Preconditions.checkNotNull(dataSet);
        Preconditions.checkNotNull(sinkName);
        Preconditions.checkNotNull(outputFormat);

        DtOutputFormatSinkFunction sinkFunction = new DtOutputFormatSinkFunction(outputFormat);
        DataStreamSink<?> dataStreamSink = dataSet.addSink(sinkFunction);
        dataStreamSink.name(sinkName);

        return dataStreamSink;
    }

    protected DataStreamSink<?> createOutput(DataStream<?> dataSet, OutputFormat outputFormat) {
        return createOutput(dataSet, outputFormat, this.getClass().getSimpleName().toLowerCase());
    }

}
