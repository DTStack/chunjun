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
package com.dtstack.flinkx.carbondata.reader;

import com.dtstack.flinkx.carbondata.CarbonConfigKeys;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Carbondata reader plugin
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbondataReader extends BaseDataReader {

    private static Logger LOG = LoggerFactory.getLogger(CarbondataReader.class);

    protected String table;

    protected String database;

    protected String path;

    protected Map<String,String> hadoopConfig;

    protected String defaultFs;

    protected List<String> columnName;

    protected List<String> columnType;

    protected List<String> columnValue;

    protected String filter;


    public CarbondataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        hadoopConfig = (Map<String, String>) readerConfig.getParameter().getVal(CarbonConfigKeys.KEY_HADOOP_CONFIG);
        table = readerConfig.getParameter().getStringVal(CarbonConfigKeys.KEY_TABLE);
        database = readerConfig.getParameter().getStringVal(CarbonConfigKeys.KEY_DATABASE);
        path = readerConfig.getParameter().getStringVal(CarbonConfigKeys.KEY_TABLE_PATH);
        filter = readerConfig.getParameter().getStringVal(CarbonConfigKeys.KEY_FILTER);
        defaultFs = readerConfig.getParameter().getStringVal(CarbonConfigKeys.KEY_DEFAULT_FS);
        List columns = readerConfig.getParameter().getColumn();

        if (columns != null && columns.size() > 0) {
            if (columns.get(0) instanceof Map) {
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
        } else {
            throw new IllegalArgumentException("column argument error");
        }
    }

    @Override
    public DataStream<Row> readData() {
        CarbondataInputFormatBuilder builder = new CarbondataInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setColumnNames(columnName);
        builder.setColumnTypes(columnType);
        builder.setColumnValues(columnValue);
        builder.setDatabase(database);
        builder.setTable(table);
        builder.setPath(path);
        builder.setDefaultFs(defaultFs);
        builder.setFilter(filter);
        builder.setHadoopConfig(hadoopConfig);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);
        builder.setLogConfig(logConfig);
        builder.setTestConfig(testConfig);
        return createInput(builder.finish());
    }

}