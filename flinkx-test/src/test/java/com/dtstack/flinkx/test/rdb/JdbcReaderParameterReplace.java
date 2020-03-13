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


package com.dtstack.flinkx.test.rdb;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.flinkx.test.core.ParameterReplace;

/**
 * @author jiangbo
 * @date 2020/2/19
 */
public class JdbcReaderParameterReplace implements ParameterReplace {

    private JSONObject connectionConfig;

    public JdbcReaderParameterReplace(JSONObject connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    @Override
    public String replaceParameter(String job) {
        if (connectionConfig == null || connectionConfig.isEmpty()) {
            return job;
        }

        JSONObject jobJson = JSONObject.parseObject(job);

        JSONArray table = jobJson.getJSONObject("job")
                .getJSONArray("content")
                .getJSONObject(0).getJSONObject("reader")
                .getJSONObject("parameter").getJSONArray("table");

        connectionConfig.put("table", table);

        JSONArray connections = new JSONArray();
        connections.add(connectionConfig);

        jobJson.getJSONObject("job")
                .getJSONArray("content")
                .getJSONObject(0).getJSONObject("reader")
                .getJSONObject("parameter")
                .put("connection", connections);

        jobJson.getJSONObject("job")
                .getJSONArray("content")
                .getJSONObject(0).getJSONObject("reader")
                .getJSONObject("parameter")
                .putAll(connectionConfig);

        return jobJson.toJSONString();
    }
}
