/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.jdbc.config;

import com.dtstack.chunjun.lookup.config.LookupConfig;

import java.util.Map;

public class JdbcLookupConfig extends LookupConfig {

    private static final long serialVersionUID = 1468075604599487858L;

    /** vertx pool size */
    protected int asyncPoolSize = 5;

    protected Map<String, Object> druidConfig;

    public static JdbcLookupConfig build() {
        return new JdbcLookupConfig();
    }

    public Map<String, Object> getDruidConfig() {
        return druidConfig;
    }

    public JdbcLookupConfig setDruidConfig(Map<String, Object> druidConfig) {
        this.druidConfig = druidConfig;
        return this;
    }

    public int getAsyncPoolSize() {
        return asyncPoolSize;
    }

    public JdbcLookupConfig setAsyncPoolSize(int asyncPoolSize) {
        this.asyncPoolSize = asyncPoolSize;
        return this;
    }
}
