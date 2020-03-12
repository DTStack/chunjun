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


package com.dtstack.flinkx.test.core;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

/**
 * @author jiangbo
 * @date 2020/2/20
 */
public class DefaultParameterReplace implements ParameterReplace {

    private JSONObject connectionConfig;

    private String pluginType;

    private String key;

    public DefaultParameterReplace(JSONObject connectionConfig, String pluginType) {
        this.connectionConfig = connectionConfig;
        this.pluginType = pluginType;
    }

    public DefaultParameterReplace(JSONObject connectionConfig, String pluginType, String key) {
        this.connectionConfig = connectionConfig;
        this.pluginType = pluginType;
        this.key = key;
    }

    @Override
    public String replaceParameter(String job) {
        if (connectionConfig == null || connectionConfig.isEmpty()) {
            return job;
        }

        JSONObject jobJson = JSONObject.parseObject(job);

        if (StringUtils.isEmpty(key)) {
            jobJson.getJSONObject("job")
                    .getJSONArray("content")
                    .getJSONObject(0).getJSONObject(pluginType)
                    .getJSONObject("parameter")
                    .putAll(connectionConfig);
        } else {
            jobJson.getJSONObject("job")
                    .getJSONArray("content")
                    .getJSONObject(0).getJSONObject(pluginType)
                    .getJSONObject("parameter")
                    .put(key, connectionConfig);
        }

        return jobJson.toJSONString();
    }
}
