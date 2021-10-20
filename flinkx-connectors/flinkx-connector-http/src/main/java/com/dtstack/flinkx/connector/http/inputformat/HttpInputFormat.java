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
package com.dtstack.flinkx.connector.http.inputformat;

import com.dtstack.flinkx.connector.http.client.HttpClient;
import com.dtstack.flinkx.connector.http.client.HttpRequestParam;
import com.dtstack.flinkx.connector.http.client.ResponseValue;
import com.dtstack.flinkx.connector.http.common.HttpRestConfig;
import com.dtstack.flinkx.connector.http.common.MetaParam;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.source.format.BaseRichInputFormat;
import com.dtstack.flinkx.throwable.ReadRecordException;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.List;

/**
 * @author : shifang
 * @date : 2020/3/12
 */
public class HttpInputFormat extends BaseRichInputFormat {

    /** 是否读取结束 */
    protected boolean reachEnd;

    /** 执行请求客户端 */
    protected HttpClient myHttpClient;

    protected HttpRestConfig httpRestConfig;

    /** 原始请求参数body */
    protected List<MetaParam> metaBodys;

    /** 原始请求参数param */
    protected List<MetaParam> metaParams;

    /** 原始请求header */
    protected List<MetaParam> metaHeaders;

    /** 读取的最新数据，checkpoint时保存 */
    protected ResponseValue state;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        reachEnd = false;
        initPosition();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void openInternal(InputSplit inputSplit) {
        myHttpClient = new HttpClient(httpRestConfig, metaBodys, metaParams, metaHeaders);
        if (state != null) {
            myHttpClient.initPosition(state.getRequestParam(), state.getOriginResponseValue());
        }

        myHttpClient.start();
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        ResponseValue value = myHttpClient.takeEvent();
        if (null == value) {
            return null;
        }
        if (value.isNormal()) {
            // 如果status是0代表是触发了异常策略stop，reachEnd更新为true
            if (value.getStatus() == 0) {
                throw new RuntimeException(
                        "the strategy ["
                                + value.getErrorMsg()
                                + " ] is triggered ，and the request param is ["
                                + value.getRequestParam().toString()
                                + "]"
                                + " and the response value is "
                                + value.getOriginResponseValue()
                                + " job end");
            }
            // todo 离线任务后期需要加上一个finished策略 这样就是代表任务正常结束 而不是异常stop
            state =
                    new ResponseValue(
                            "",
                            HttpRequestParam.copy(value.getRequestParam()),
                            value.getOriginResponseValue());
            try {
                return rowConverter.toInternal(value.getData());
            } catch (Exception e) {
                throw new ReadRecordException(e.getMessage(), e);
            }
        } else {
            throw new RuntimeException("request data error,msg is " + value.getErrorMsg());
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] inputSplits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return inputSplits;
    }

    private void initPosition() {
        if (null != formatState && formatState.getState() != null) {
            state = ((ResponseValue) formatState.getState());
        }
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();

        if (formatState != null) {
            formatState.setState(state);
        }

        return formatState;
    }

    @Override
    protected void closeInternal() {
        myHttpClient.close();
    }

    @Override
    public boolean reachedEnd() {
        return reachEnd;
    }

    public void setHttpRestConfig(HttpRestConfig httpRestConfig) {
        this.httpRestConfig = httpRestConfig;
    }
}
