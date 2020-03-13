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

package com.dtstack.flinkx.es.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.es.EsConfigKeys;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The writer plugin of ElasticSearch
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class EsWriter extends BaseDataWriter {

    public static final int DEFAULT_BULK_ACTION = 100;

    private String address;
    private String username;
    private String password;
    private String index;
    private String type;
    private int bulkAction;
    private Map<String,Object> clientConfig;
    private List<String> columnTypes;
    private List<String> columnNames;
    private List<Integer> idColumnIndices;
    private List<String> idColumnTypes;
    private List<String> idColumnValues;

    public EsWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        address = writerConfig.getParameter().getStringVal(EsConfigKeys.KEY_ADDRESS);
        username = writerConfig.getParameter().getStringVal(EsConfigKeys.KEY_USERNAME);
        password = writerConfig.getParameter().getStringVal(EsConfigKeys.KEY_PASSWORD);
        type = writerConfig.getParameter().getStringVal(EsConfigKeys.KEY_TYPE);
        index = writerConfig.getParameter().getStringVal(EsConfigKeys.KEY_INDEX);
        bulkAction = writerConfig.getParameter().getIntVal(EsConfigKeys.KEY_BULK_ACTION, DEFAULT_BULK_ACTION);

        clientConfig = new HashMap<>();
        clientConfig.put(EsConfigKeys.KEY_TIMEOUT, writerConfig.getParameter().getVal(EsConfigKeys.KEY_TIMEOUT));
        clientConfig.put(EsConfigKeys.KEY_PATH_PREFIX, writerConfig.getParameter().getVal(EsConfigKeys.KEY_PATH_PREFIX));

        List columns = writerConfig.getParameter().getColumn();
        if(CollectionUtils.isNotEmpty(columns)) {
            columnTypes = new ArrayList<>();
            columnNames = new ArrayList<>();
            for(int i = 0; i < columns.size(); ++i) {
                Map sm = (Map) columns.get(i);
                columnNames.add((String) sm.get(EsConfigKeys.KEY_COLUMN_NAME));
                columnTypes.add((String) sm.get(EsConfigKeys.KEY_COLUMN_TYPE));
            }
        }

        List idColumns = (List) writerConfig.getParameter().getVal(EsConfigKeys.KEY_ID_COLUMN);
        if( idColumns != null &&  idColumns.size() != 0) {
            idColumnIndices = new ArrayList<>();
            idColumnTypes = new ArrayList<>();
            idColumnValues = new ArrayList<>();
            for(int i = 0; i <  idColumns.size(); ++i) {
                Map<String,Object> sm =
                        (Map)  idColumns.get(i);
                Object ind = sm.get(EsConfigKeys.KEY_ID_COLUMN_INDEX);
                if(ind == null) {
                    idColumnIndices.add(-1);
                } else if(ind instanceof Double) {
                    idColumnIndices.add(((Double) ind).intValue());
                } else if (ind instanceof Integer) {
                    idColumnIndices.add(((Integer) ind).intValue());
                } else if (ind instanceof Long) {
                    idColumnIndices.add(((Long) ind).intValue());
                } else if(ind instanceof String) {
                    idColumnIndices.add(Integer.valueOf((String) ind));
                }
                idColumnTypes.add((String) sm.get(EsConfigKeys.KEY_ID_COLUMN_TYPE));
                idColumnValues.add((String) sm.get(EsConfigKeys.KEY_ID_COLUMN_VALUE));
            }
        }

    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        EsOutputFormatBuilder builder = new EsOutputFormatBuilder();
        builder.setAddress(address);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setIndex(index);
        builder.setType(type);
        builder.setBatchInterval(bulkAction);
        builder.setClientConfig(clientConfig);
        builder.setColumnNames(columnNames);
        builder.setColumnTypes(columnTypes);
        builder.setIdColumnIndices(idColumnIndices);
        builder.setIdColumnTypes(idColumnTypes);
        builder.setIdColumnValues(idColumnValues);
        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);

        return createOutput(dataSet, builder.finish());
    }
}
