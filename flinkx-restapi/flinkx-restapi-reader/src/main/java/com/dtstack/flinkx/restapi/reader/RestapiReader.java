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
import com.dtstack.flinkx.restapi.common.ConstantValue;
import com.dtstack.flinkx.restapi.common.HttpMethod;
import com.dtstack.flinkx.restapi.common.MetaParam;
import com.dtstack.flinkx.restapi.common.ParamType;
import com.dtstack.flinkx.restapi.format.RestapiInputFormatBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Collections;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 */
public class RestapiReader extends BaseDataReader {

    /**
     * http的请求参数
     **/
    private HttpRestConfig httpRestConfig;


    @SuppressWarnings("unchecked")
    public RestapiReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        try {
            this.httpRestConfig = objectMapper.readValue(objectMapper.writeValueAsString(readerConfig.getParameter().getAll()), HttpRestConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("analyze httpRest Config failed:", e);
        }

        MetaParam.setMetaColumnsType(httpRestConfig.getBody(), ParamType.BODY);
        MetaParam.setMetaColumnsType(httpRestConfig.getParam(), ParamType.PARAM);
        MetaParam.setMetaColumnsType(httpRestConfig.getHeader(), ParamType.HEADER);

        MetaParam.initTimeFormat(httpRestConfig.getBody());
        MetaParam.initTimeFormat(httpRestConfig.getParam());
        MetaParam.initTimeFormat(httpRestConfig.getHeader());

        //post请求 如果contentTy没有设置，则默认设置为 application/json
        if(HttpMethod.POST.name().equalsIgnoreCase(httpRestConfig.getRequestMode()) && httpRestConfig.getHeader().stream().noneMatch(i->ConstantValue.CONTENT_TYPE_NAME.equals(i.getKey()))){
            if(CollectionUtils.isEmpty(httpRestConfig.getHeader())){
                httpRestConfig.setHeader( Collections.singletonList(new MetaParam(ConstantValue.CONTENT_TYPE_NAME, ConstantValue.CONTENT_TYPE_DEFAULT_VALUE, ParamType.HEADER)));
            }else{
                httpRestConfig.getHeader().add(new MetaParam(ConstantValue.CONTENT_TYPE_NAME, ConstantValue.CONTENT_TYPE_DEFAULT_VALUE, ParamType.HEADER));
            }
        }

    }

    @Override
    public DataStream<Row> readData() {
        RestapiInputFormatBuilder builder = new RestapiInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);

        builder.setMetaHeaders(httpRestConfig.getHeader());
        builder.setMetaParams(httpRestConfig.getParam());
        builder.setMetaBodys(httpRestConfig.getBody());
        builder.setHttpRestConfig(httpRestConfig);
        builder.setStream(restoreConfig.isStream());

        builder.setDataTransferConfig(dataTransferConfig);
        builder.setRestoreConfig(restoreConfig);

        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);
        return createInput(builder.finish());
    }
}
