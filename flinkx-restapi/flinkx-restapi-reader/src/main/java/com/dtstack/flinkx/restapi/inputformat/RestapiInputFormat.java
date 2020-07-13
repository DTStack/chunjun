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
import com.dtstack.flinkx.restapi.common.HttpUtil;
import com.dtstack.flinkx.util.GsonUtil;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 */
public class RestapiInputFormat extends BaseRichInputFormat {

    protected String url;

    protected String method;

    protected transient CloseableHttpClient httpClient;

    protected  Map<String, Object> header;

    protected  Map<String, Object> entityDataToMap;

    protected boolean getData;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        httpClient = HttpUtil.getHttpClient();
    }

    @Override
    public void closeInputFormat() {
        HttpUtil.closeClient(httpClient);
    }


    @Override
    @SuppressWarnings("unchecked")
    protected void openInternal(InputSplit inputSplit) throws IOException {
        HttpUriRequest request = HttpUtil.getRequest(method, header,null, url);
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(request);
            HttpEntity entity = httpResponse.getEntity();
            if (entity != null) {
                String entityData = EntityUtils.toString(entity);
                entityDataToMap = GsonUtil.GSON.fromJson(entityData, Map.class);
                getData = true;
            } else {
                throw new RuntimeException("entity is null");
            }
        } catch (Exception e) {
            throw new RuntimeException("get entity error");
        }
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
        row = new Row(1);
        row.setField(0, entityDataToMap);
        getData = false;
        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !getData;
    }
}
