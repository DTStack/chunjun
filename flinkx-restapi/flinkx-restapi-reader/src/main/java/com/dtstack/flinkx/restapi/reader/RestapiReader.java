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
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.restapi.common.InnerVaribleFactory;
import com.dtstack.flinkx.restapi.common.IntervalTimeVarible;
import com.dtstack.flinkx.restapi.common.handler.DataHandlerFactory;
import com.dtstack.flinkx.restapi.inputformat.RestapiInputFormatBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 */
public class RestapiReader extends BaseDataReader {

    private HttpRestConfig httpRestConfig;

    private List<MetaColumn> metaColumns;

    protected Long intervalTime;

    protected List handlers;

    @SuppressWarnings("unchecked")
    public RestapiReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        try {
            this.httpRestConfig = objectMapper.readValue(objectMapper.writeValueAsString(readerConfig.getParameter().getAll()), HttpRestConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("解析httpRest Config配置出错:", e);
        }
        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());
        List handlers = httpRestConfig.getHandlers();
        if (CollectionUtils.isNotEmpty(handlers)) {
            handlers = new ArrayList(handlers.size() * 2);
            for (Object handlerParam : handlers) {
                try {
                    handlers.add(DataHandlerFactory.getDataHandler((Map) handlerParam));
                } catch (Exception e) {
                    throw new IllegalArgumentException("dataHandler param error" + httpRestConfig.getHandlers());
                }
            }
            DataHandlerFactory.destroy();
        }

        InnerVaribleFactory.addVarible("intervalTime", new IntervalTimeVarible(httpRestConfig.getIntervalTime()));
        intervalTime = httpRestConfig.getIntervalTime();

    }

    @Override
    public DataStream<Row> readData() {
        RestapiInputFormatBuilder builder = new RestapiInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setIntervalTime(intervalTime);
        builder.setMetaColumns(metaColumns);
        builder.setHandlers(handlers);
        builder.setHttpRestConfig(httpRestConfig);


        return createInput(builder.finish());
    }
}
