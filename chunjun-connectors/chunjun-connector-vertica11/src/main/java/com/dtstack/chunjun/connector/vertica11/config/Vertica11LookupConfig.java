/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.vertica11.config;

import com.dtstack.chunjun.connector.jdbc.config.JdbcLookupConfig;

public class Vertica11LookupConfig extends JdbcLookupConfig {

    private static final long serialVersionUID = 1381371803169634926L;

    /** vertx pool size */
    protected int asyncPoolSize = 5;

    public static Vertica11LookupConfig build() {
        return new Vertica11LookupConfig();
    }

    public int getAsyncPoolSize() {
        return asyncPoolSize;
    }

    public Vertica11LookupConfig setAsyncPoolSize(int asyncPoolSize) {
        this.asyncPoolSize = asyncPoolSize;
        return this;
    }
}
