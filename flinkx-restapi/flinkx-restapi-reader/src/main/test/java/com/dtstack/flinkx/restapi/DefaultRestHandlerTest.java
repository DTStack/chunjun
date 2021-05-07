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

package com.dtstack.flinkx.restapi;

import com.dtstack.flinkx.restapi.client.DefaultRestHandler;
import com.dtstack.flinkx.restapi.client.HttpRequestParam;
import com.dtstack.flinkx.restapi.client.ResponseValue;
import com.dtstack.flinkx.restapi.client.Strategy;
import com.dtstack.flinkx.restapi.common.ConstantValue;
import com.dtstack.flinkx.restapi.common.MetaParam;
import com.dtstack.flinkx.restapi.common.ParamType;
import com.dtstack.flinkx.restapi.reader.HttpRestConfig;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.SnowflakeIdWorker;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

public class DefaultRestHandlerTest {
    DefaultRestHandler defaultRestHandler = new DefaultRestHandler();


    @Test
    public void buildRequestParam() {
        ArrayList<MetaParam> metaBodyList = new ArrayList<>();

        MetaParam param = new MetaParam("param.key1.key2", "1", ParamType.BODY, true);
        param.setNextValue("${body.param.key1.key2}+1");

        metaBodyList.add(param);

        HttpRestConfig httpRestConfig = new HttpRestConfig();
        HttpRequestParam httpRequestParam = defaultRestHandler.buildRequestParam(Collections.emptyList(), metaBodyList, Collections.emptyList(), new HttpRequestParam(), Collections.emptyMap(), httpRestConfig, true);
        HttpRequestParam httpRequestParam1 = defaultRestHandler.buildRequestParam(Collections.emptyList(), metaBodyList, Collections.emptyList(), httpRequestParam, Collections.emptyMap(), httpRestConfig, false);

        Assert.assertEquals(Integer.parseInt(httpRequestParam.getValue(param, httpRestConfig.getFieldDelimiter()).toString()), 1);
        Assert.assertEquals(Integer.parseInt(httpRequestParam1.getValue(param, httpRestConfig.getFieldDelimiter()).toString()), 2);


    }

    @Test
    public void chooseStrategy() {
        String responseValue = "{\"data\":{\"key1\":\"v1\",\"k2\":\"v2\"}}";
        Map map = GsonUtil.GSON.fromJson(responseValue, Map.class);

        ArrayList<Strategy> strategies = new ArrayList<>();
        Strategy strategy = new Strategy();
        strategy.setKey("${response.data.key1}");
        strategy.setValue("v1");
        strategy.setHandle("stop");
        strategies.add(strategy);

        MetaParam param = new MetaParam("param.key1.key2", "1", ParamType.BODY, true);
        param.setNextValue("${body.param.key1.key2}+1");


        Strategy strategy1 = defaultRestHandler.chooseStrategy(strategies, map, new HttpRestConfig(), new HttpRequestParam(), Collections.singletonList(param));
        Assert.assertEquals(strategy1, strategy);
    }

    @Test
    public void buildResponseValue() {

        String responseValue = "{\"data\":{\"key1\":\"v1\",\"k2\":\"v2\"}}";
        String fields = "data.key1";
        ResponseValue responseValue1 = defaultRestHandler.buildResponseValue(ConstantValue.DEFAULT_DECODE, responseValue, fields, new HttpRequestParam());
        Assert.assertEquals(responseValue1.getOriginResponseValue(), responseValue);
        Assert.assertEquals(responseValue1.getData(), "{\"data\":{\"key1\":\"v1\"}}");
    }


}
