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
import com.dtstack.flinkx.restapi.common.RestapiKeys;
import com.dtstack.flinkx.restapi.outputformat.RestapiOutputFormatBuilder;
import com.dtstack.flinkx.writer.BaseDataWriter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
public class RestapiWriter extends BaseDataWriter {

    protected String url;

    protected String method;

    protected Map<String, String> header = Maps.newHashMap();

    protected Map<String, Object> body =Maps.newHashMap();

    protected ArrayList<String> column = Lists.newArrayList();

    protected Map<String, Object> params = Maps.newHashMap();

    protected int batchInterval;

    @SuppressWarnings("unchecked")
    public RestapiWriter(DataTransferConfig config) {
        super(config);
        Object tempObj;

        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();

        url = writerConfig.getParameter().getStringVal(RestapiKeys.KEY_URL);
        method = writerConfig.getParameter().getStringVal(RestapiKeys.KEY_METHOD);
        batchInterval = writerConfig.getParameter().getIntVal(RestapiKeys.KEY_BATCH_INTERVAL, 1);
        tempObj = writerConfig.getParameter().getVal(RestapiKeys.KEY_COLUMN);
        if (tempObj != null) {
            column.addAll((ArrayList<String>) tempObj);
        }

        tempObj = writerConfig.getParameter().getVal(RestapiKeys.KEY_HEADER);
        if (tempObj != null) {
            for (Map<String, String> map : (ArrayList<Map<String, String>>) tempObj) {
                header.putAll(map);
            }
        }

        tempObj = writerConfig.getParameter().getVal(RestapiKeys.KEY_BODY);
        if (tempObj != null) {
            for (Map<String, Object> map : (ArrayList<Map<String, Object>>) tempObj) {
                body.putAll(map);
            }
        }
        tempObj = writerConfig.getParameter().getVal(RestapiKeys.KEY_PARAMS);
        if (tempObj != null) {
            params = (HashMap<String, Object>) tempObj;
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        RestapiOutputFormatBuilder builder = new RestapiOutputFormatBuilder();

        builder.setHeader(header);
        builder.setMethod(method);
        builder.setUrl(url);
        builder.setBody(body);
        builder.setColumn(column);
        builder.setParams(params);
        builder.setBatchInterval(batchInterval);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);

        return createOutput(dataSet, builder.finish());
    }
}
