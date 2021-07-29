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

package com.dtstack.flinkx.es.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.es.EsConfigKeys;
import com.dtstack.flinkx.es.EsUtil;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.google.gson.Gson;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Reader plugin of ElasticSearch
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class EsReader extends BaseDataReader {

    private static Logger LOG = LoggerFactory.getLogger(EsReader.class);

    private String address;
    private String username;
    private String password;
    private String query;

    private String[] index;
    private String[] type;
    private Integer batchSize;
    private Map<String,Object> clientConfig;

    protected List<String> columnType;
    protected List<String> columnValue;
    protected List<String> columnName;

    public EsReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        address = readerConfig.getParameter().getStringVal(EsConfigKeys.KEY_ADDRESS);
        username = readerConfig.getParameter().getStringVal(EsConfigKeys.KEY_USERNAME);
        password = readerConfig.getParameter().getStringVal(EsConfigKeys.KEY_PASSWORD);
        index = EsUtil.getStringArray(readerConfig.getParameter().getVal(EsConfigKeys.KEY_INDEX));
        type = EsUtil.getStringArray(readerConfig.getParameter().getVal(EsConfigKeys.KEY_TYPE));
        batchSize = readerConfig.getParameter().getIntVal(EsConfigKeys.KEY_BATCH_SIZE, 10);

        clientConfig = new HashMap<>();
        clientConfig.put(EsConfigKeys.KEY_TIMEOUT, readerConfig.getParameter().getVal(EsConfigKeys.KEY_TIMEOUT));
        clientConfig.put(EsConfigKeys.KEY_PATH_PREFIX, readerConfig.getParameter().getVal(EsConfigKeys.KEY_PATH_PREFIX));

        Object queryMap = readerConfig.getParameter().getVal(EsConfigKeys.KEY_QUERY);
        if(queryMap != null) {
            query = new Gson().toJson(queryMap);
        }

        List columns = readerConfig.getParameter().getColumn();
        if(columns != null && columns.size() > 0) {
            if(columns.get(0) instanceof Map) {
                columnType = new ArrayList<>();
                columnValue = new ArrayList<>();
                columnName = new ArrayList<>();
                for(int i = 0; i < columns.size(); ++i) {
                    Map sm = (Map) columns.get(i);
                    columnType.add((String) sm.get("type"));
                    columnValue.add((String) sm.get("value"));
                    columnName.add((String) sm.get("name"));
                }

                LOG.info("init column finished");
            } else if (!ConstantValue.STAR_SYMBOL.equals(columns.get(0)) || columns.size() != 1) {
                throw new IllegalArgumentException("column argument error");
            }
        } else{
            throw new IllegalArgumentException("column argument error");
        }
    }

    @Override
    public DataStream<Row> readData() {
        EsInputFormatBuilder builder = new EsInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setColumnNames(columnName);
        builder.setColumnTypes(columnType);
        builder.setColumnValues(columnValue);
        builder.setAddress(address);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setIndex(index);
        builder.setType(type);
        builder.setBatchSize(batchSize);
        builder.setClientConfig(clientConfig);
        builder.setQuery(query);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);
        builder.setTestConfig(testConfig);
        builder.setLogConfig(logConfig);

        return createInput(builder.finish());
    }

}
