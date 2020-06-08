/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.metadata.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.metadata.inputformat.MetadataInputFormatBuilder;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import com.dtstack.flinkx.metadata.MetaDataCons;

import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 */
public class MetadataReader extends BaseDataReader {
    protected String jdbcUrl;
    protected List<Map<String, Object>> dbList;
    protected String username;
    protected String password;
    protected String driverName;

    @SuppressWarnings("unchecked")
    protected MetadataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        jdbcUrl = readerConfig.getParameter().getStringVal(MetaDataCons.KEY_JDBC_URL);
        username = readerConfig.getParameter().getStringVal(MetaDataCons.KEY_CONN_USERNAME);
        password = readerConfig.getParameter().getStringVal(MetaDataCons.KEY_CONN_PASSWORD);
        dbList = (List<Map<String, Object>>) readerConfig.getParameter().getVal(MetaDataCons.KEY_DB_LIST);

    }

    @Override
    public DataStream<Row> readData() {
        MetadataInputFormatBuilder builder = getBuilder();

        builder.setDbUrl(jdbcUrl);
        builder.setPassword(password);
        builder.setUsername(username);
        builder.setDriverName(driverName);
        builder.setDbList(dbList);

        BaseRichInputFormat format = builder.finish();

        return createInput(format);
    }

    protected MetadataInputFormatBuilder getBuilder(){
        throw new RuntimeException("子类必须覆盖getBuilder方法");
    }
}
