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
import com.dtstack.metadata.rdb.core.entity.ConnectionInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_PASSWORD;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_USERNAME;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_URL;

/**
 * @author kunni@dtstack.com
 */
abstract public class MetadatardbReader extends MetaDataBaseReader {

    protected ConnectionInfo connectionInfo;

    protected MetadatardbReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        connectionInfo =new ConnectionInfo();
        connectionInfo.setDriver(getDriverName());
        connectionInfo.setJdbcUrl(params.getStringVal(KEY_URL));
        connectionInfo.setPassword(params.getStringVal(KEY_PASSWORD));
        connectionInfo.setUsername(params.getStringVal(KEY_USERNAME));
    }

    @Override
    public DataStream<Row> readData() {
        MetadatardbBuilder builder = createBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setOriginalJob(originalJob);
        builder.setConnectionInfo(connectionInfo);
        return createInput(builder.finish());
    }

    /**
     * 重写该方法，返回builder
     * @return builder
     */
    @Override
    abstract public MetadatardbBuilder createBuilder();

    /**
     * 设置驱动
     * @return 驱动类名
     */
    abstract public String getDriverName();
}
