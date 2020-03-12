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


package com.dtstack.flinkx.test.core.source;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jiangbo
 * @date 2020/2/19
 */
public class StaticDataSource implements DataSource {

    public static Logger LOG = LoggerFactory.getLogger(StaticDataSource.class);

    private JSONObject config;

    public StaticDataSource(JSONObject config) {
        this.config = config;
    }

    @Override
    public JSONObject prepare() {
        if (config == null || config.isEmpty()) {
            LOG.warn("[config]为null或者为空,返回空JSON对象");
            return new JSONObject();
        }

        return config;
    }
}
