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
package com.dtstack.flinkx.restapi.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.restapi.outputformat.RestAPIOutputFormatBuilder;
import com.dtstack.flinkx.util.JsonUtils;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 */
public class RestapiWriter extends DataWriter {
    protected String url;
    protected String method;
    protected Map<String, String> header = new HashMap<>();
    protected Map<String, Object> body = new HashMap<>();
    protected ArrayList<Map<String, Object>> temp;
    protected ArrayList<Map<String, String>> tempHeader;
    protected ArrayList<String> column;
    protected Map<String, Object> params;

    public RestapiWriter(DataTransferConfig config) {
        super(config);

        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();

        url = writerConfig.getParameter().getStringVal("url");
        method = writerConfig.getParameter().getStringVal("method");
        column = (ArrayList<String>) writerConfig.getParameter().getVal("column");

        tempHeader = (ArrayList<Map<String, String>>) writerConfig.getParameter().getVal("header");

        for (Map<String, String> map : tempHeader) {
            header.putAll(map);
        }
        temp = (ArrayList<Map<String, Object>>) writerConfig.getParameter().getVal("body");

        for (Map<String, Object> map : temp) {
            body.putAll(map);
        }
        params = (HashMap) writerConfig.getParameter().getVal("params");
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        RestAPIOutputFormatBuilder builder = new RestAPIOutputFormatBuilder();

        builder.setHeader(header);
        builder.setMethod(method);
        builder.setUrl(url);
        builder.setBody(body);
        builder.setColumn(column);
        builder.setParams(params);

        return createOutput(dataSet, builder.finish());
    }
}
