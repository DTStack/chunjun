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
package com.dtstack.flinkx.restapi.inputformat;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.restapi.common.ParamType;
import com.dtstack.flinkx.restapi.common.RestContext;
import com.dtstack.flinkx.restapi.common.handler.DataHandler;
import com.dtstack.flinkx.restapi.common.handler.DataHandlerFactory;
import com.dtstack.flinkx.restapi.reader.HttpRestConfig;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 */
public class RestapiInputFormat extends BaseRichInputFormat {

    protected HttpClient myHttpClient;

    protected RestContext restContext;

    protected Long intervalTime;

    protected HttpRestConfig httpRestConfig;

    protected List<MetaColumn> metaColumns ;

    protected List<DataHandler> handlers;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
    }



    @Override
    @SuppressWarnings("unchecked")
    protected void openInternal(InputSplit inputSplit) throws IOException {
        restContext = new RestContext(httpRestConfig.getType(),httpRestConfig.getUrl(),httpRestConfig.getFormat());
        myHttpClient = new HttpClient(restContext, intervalTime);
        myHttpClient.setMetaColumns(metaColumns);
        myHttpClient.setHandlers(handlers);
        restContext.parseAndInt(httpRestConfig.getBody(), ParamType.BODY);
        restContext.parseAndInt(httpRestConfig.getHeader(), ParamType.HEADER);
        restContext.parseAndInt(httpRestConfig.getParam(), ParamType.PARAM);

        myHttpClient.start();
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        InputSplit[] inputSplits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return myHttpClient.takeEvent();

    }

    @Override
    protected void closeInternal() throws IOException {
        myHttpClient.close();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    public void setHttpRestConfig(HttpRestConfig httpRestConfig) {
        this.httpRestConfig = httpRestConfig;
    }

    public void setIntervalTime(Long intervalTime) {
        this.intervalTime = intervalTime;
    }

    public void setMetaColumns(List<MetaColumn> metaColumns) {
        this.metaColumns = metaColumns;
    }

    public List<DataHandler> getHandlers() {
        return handlers;
    }

    public void setHandlers(List<DataHandler> handlers) {
        this.handlers = handlers;
    }
}
