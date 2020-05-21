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
package com.dtstack.flinkx.restapi.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.restapi.inputformat.RestapiInputFormatBuilder;
import com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 */
public class RestapiReader extends BaseDataReader {

    private String url;

    private String method;

    private Map<String, Object> header = Maps.newHashMap();

    private ArrayList<Map<String, String>> temp;

    @SuppressWarnings("unchecked")
    public RestapiReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        url = readerConfig.getParameter().getStringVal("url");
        method = readerConfig.getParameter().getStringVal("method");
        temp = (ArrayList<Map<String, String>>) readerConfig.getParameter().getVal("header");
        if (temp != null) {
            for (Map<String, String> map : temp) {
                header.putAll(map);
            }
        }
    }

    @Override
    public DataStream<Row> readData() {
        RestapiInputFormatBuilder builder = new RestapiInputFormatBuilder();

        builder.setHeader(header);
        builder.setMethod(method);
        builder.setUrl(url);

        return createInput(builder.finish());
    }
}
