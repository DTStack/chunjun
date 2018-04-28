/**
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

package com.dtstack.flinkx.es.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.es.EsConfigKeys;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The writer plugin of ElasticSearch
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class EsWriter extends DataWriter {

    public static final int DEFAULT_BULK_ACTION = 100;

    private String address;

    private String type;

    private int bulkAction;

    private List<String> columnTypes;

    private List<String> columnNames;

    private List<Integer> indexColumnIndices;

    private List<String> indexColumnTypes;

    private List<String> indexColumnValues;

    public EsWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        address = writerConfig.getParameter().getStringVal(EsConfigKeys.KEY_ADDRESS);
        type = writerConfig.getParameter().getStringVal(EsConfigKeys.KEY_TYPE);
        bulkAction = writerConfig.getParameter().getIntVal(EsConfigKeys.KEY_BULK_ACTION, DEFAULT_BULK_ACTION);

        List columns = writerConfig.getParameter().getColumn();
        if(columns != null || columns.size() != 0) {
            columnTypes = new ArrayList<>();
            columnNames = new ArrayList<>();
            for(int i = 0; i < columns.size(); ++i) {
                Map sm = (Map) columns.get(i);
                columnNames.add((String) sm.get(EsConfigKeys.KEY_COLUMN_NAME));
                columnTypes.add((String) sm.get(EsConfigKeys.KEY_COLUMN_TYPE));
            }
        }

        List indexColumns = (List) writerConfig.getParameter().getVal(EsConfigKeys.KEY_INDEX_COLUMN);
        if(indexColumns != null || indexColumns.size() != 0) {
            indexColumnIndices = new ArrayList<>();
            indexColumnTypes = new ArrayList<>();
            indexColumnValues = new ArrayList<>();
            for(int i = 0; i < indexColumns.size(); ++i) {
                Map<String,Object> sm = (Map) indexColumns.get(i);
                Object ind = sm.get(EsConfigKeys.KEY_INDEX_COLUMN_INDEX);
                if(ind instanceof Double) {
                    indexColumnIndices.add(((Double) ind).intValue());
                } else if (ind instanceof Integer) {
                    indexColumnIndices.add(((Integer) ind).intValue());
                } else if (ind instanceof Long) {
                    indexColumnIndices.add(((Long) ind).intValue());
                } else if(ind instanceof String) {
                    indexColumnIndices.add(Integer.valueOf((String) ind));
                }
                indexColumnTypes.add((String) sm.get(EsConfigKeys.KEY_INDEX_COLUMN_TYPE));
                indexColumnValues.add((String) sm.get(EsConfigKeys.KEY_INDEX_COLUMN_VALUE));
            }
        }

    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        EsOutputFormatBuilder builder = new EsOutputFormatBuilder();
        builder.setAddress(address);
        builder.setType(type);
        builder.setBatchInterval(bulkAction);
        builder.setColumnNames(columnNames);
        builder.setColumnTypes(columnTypes);
        builder.setIndexColumnIndices(indexColumnIndices);
        builder.setIndexColumnTypes(indexColumnTypes);
        builder.setIndexColumnValues(indexColumnValues);
        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);

        OutputFormatSinkFunction sinkFunction = new OutputFormatSinkFunction(builder.finish());
        DataStreamSink<?> dataStreamSink = dataSet.addSink(sinkFunction);

        dataStreamSink.name("eswriter");

        return dataStreamSink;
    }
}
