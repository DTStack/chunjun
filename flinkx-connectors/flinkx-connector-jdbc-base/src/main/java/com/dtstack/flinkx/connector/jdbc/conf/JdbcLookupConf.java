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

package com.dtstack.flinkx.connector.jdbc.conf;

import com.dtstack.flinkx.lookup.conf.LookupConf;

import java.util.Map;

/**
 * @author chuixue
 * @create 2021-04-10 22:10
 * @description
 */
public class JdbcLookupConf extends LookupConf {
    /** vertx pool size */
    protected int asyncPoolSize = 5;

    protected Map<String, Object> druidConf;

    public static JdbcLookupConf build() {
        return new JdbcLookupConf();
    }

    public Map<String, Object> getDruidConf() {
        return druidConf;
    }

    public JdbcLookupConf setDruidConf(Map<String, Object> druidConf) {
        this.druidConf = druidConf;
        return this;
    }

    public int getAsyncPoolSize() {
        return asyncPoolSize;
    }

    public JdbcLookupConf setAsyncPoolSize(int asyncPoolSize) {
        this.asyncPoolSize = asyncPoolSize;
        return this;
    }
}
