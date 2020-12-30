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
package com.dtstack.flinkx.metadataes6.reader;


import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.metadata.reader.MetaDataBaseReader;
import com.dtstack.flinkx.metadataes6.builder.Metadataes6Builder;
import com.dtstack.flinkx.metadataes6.format.Metadataes6InputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import static com.dtstack.flinkx.metadataes6.constants.MetaDataEs6Cons.KEY_PASSWORD;
import static com.dtstack.flinkx.metadataes6.constants.MetaDataEs6Cons.KEY_USERNAME;
import static com.dtstack.flinkx.metadataes6.constants.MetaDataEs6Cons.KEY_URL;
/**
 * @author : baiyu
 * @date : 2020/12/30
 */
public class Metadataes6Reader extends MetaDataBaseReader {

    private String url;

    private String username;

    private String password;

    public Metadataes6Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        url = params.getStringVal(KEY_URL);
        username = params.getStringVal(KEY_USERNAME);
        password = params.getStringVal(KEY_PASSWORD);
    }

    @Override
    public DataStream<Row> readData() {
        Metadataes6Builder builder = createBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setOriginalJob(originalJob);
        builder.setPassword(password);
        builder.setUsername(username);
        builder.setUrl(url);
        return createInput(builder.finish());
    }

    @Override
    public Metadataes6Builder createBuilder() {
        return new Metadataes6Builder(new Metadataes6InputFormat());
    }
}
