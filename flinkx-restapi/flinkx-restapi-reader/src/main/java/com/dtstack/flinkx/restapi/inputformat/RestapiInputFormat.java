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
import com.dtstack.flinkx.restapi.common.MetaParam;
import com.dtstack.flinkx.restapi.reader.HttpRestConfig;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 */
public class RestapiInputFormat extends BaseRichInputFormat {


    /**
     * 是否是实时任务
     **/
    protected boolean isStream;

    protected boolean reachEnd;

    protected HttpClient myHttpClient;

    protected HttpRestConfig httpRestConfig;

    /**
     * 原始请求参数
     */
    protected List<MetaParam> metaBodys;

    /**
     * 原始请求参数
     */
    protected List<MetaParam> metaParams;

    /**
     * 原始请求header
     */
    protected List<MetaParam> metaHeaders;


    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        reachEnd = false;
    }


    @Override
    @SuppressWarnings("unchecked")
    protected void openInternal(InputSplit inputSplit) {
        myHttpClient = new HttpClient(httpRestConfig, httpRestConfig.getFields(), metaBodys, metaParams, metaHeaders);
        myHttpClient.start();
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] inputSplits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) {
        row = myHttpClient.takeEvent();
        if (null == row) {
            return null;
        }
        ResponseValue value = (ResponseValue) row.getField(0);
        if (value.isNormal()) {
            //如果status是0代表是最后一条数据，reachEnd更新为true
            if (value.getStatus() == 0) {
                reachEnd = true;
            }
            return Row.of(value.getData());
        } else {
            throw new RuntimeException("request data error,msg is " + value.getErrorMsg());
        }
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
