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

package com.dtstack.metadata.rdb.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.metadata.reader.MetaDataBaseReader;
import com.dtstack.metadata.rdb.builder.MetadatardbBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_CONN_PASSWORD;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_CONN_USERNAME;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_JDBC_URL;

/**
 * @author kunni@dtstack.com
 */
abstract public class MetadatardbReader extends MetaDataBaseReader {

    protected String url;

    protected String username;

    protected String password;

    protected String driverName;

    protected MetadatardbReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        url = params.getStringVal(KEY_JDBC_URL);
        username = params.getStringVal(KEY_CONN_USERNAME);
        password = params.getStringVal(KEY_CONN_PASSWORD);
        driverName = getDriverName();
    }

    @Override
    public DataStream<Row> readData() {
        MetadatardbBuilder builder = createBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setOriginalJob(originalJob);
        builder.setDriverName(driverName);
        builder.setPassword(password);
        builder.setUsername(username);
        builder.setUrl(url);
        return createInput(builder.finish());
    }

    /**
     * 重写该方法，返回metadatardbBuilder
     * @return metadatardbbuilder
     */
    @Override
    abstract public MetadatardbBuilder createBuilder();

    /**
     * 设置驱动
     */
    abstract public String getDriverName();
}
