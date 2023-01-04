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

package com.dtstack.chunjun.connector.cassandra.config;

import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.configuration.ReadableConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.TABLE_NAME;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ASYNC_TIMEOUT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_PERIOD;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TTL;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_CACHE_TYPE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_ERROR_LIMIT;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_FETCH_SIZE;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_MAX_RETRIES;
import static com.dtstack.chunjun.lookup.options.LookupOptions.LOOKUP_PARALLELISM;

@EqualsAndHashCode(callSuper = true)
@Data
public class CassandraLookupConfig extends LookupConfig {

    private static final long serialVersionUID = 8506224951957703543L;

    private CassandraCommonConfig commonConfig;

    public static CassandraLookupConfig from(ReadableConfig readableConfig) {
        CassandraLookupConfig config = new CassandraLookupConfig();

        CassandraCommonConfig kuduCommonConf =
                CassandraCommonConfig.from(readableConfig, new CassandraCommonConfig());
        config.setCommonConfig(kuduCommonConf);

        // common lookup
        config.setTableName(readableConfig.get(TABLE_NAME));
        config.setPeriod(readableConfig.get(LOOKUP_CACHE_PERIOD));
        config.setCacheSize(readableConfig.get(LOOKUP_CACHE_MAX_ROWS));
        config.setCacheTtl(readableConfig.get(LOOKUP_CACHE_TTL));
        config.setCache(readableConfig.get(LOOKUP_CACHE_TYPE));
        config.setMaxRetryTimes(readableConfig.get(LOOKUP_MAX_RETRIES));
        config.setErrorLimit(readableConfig.get(LOOKUP_ERROR_LIMIT));
        config.setFetchSize(readableConfig.get(LOOKUP_FETCH_SIZE));
        config.setAsyncTimeout(readableConfig.get(LOOKUP_ASYNC_TIMEOUT));
        config.setParallelism(readableConfig.get(LOOKUP_PARALLELISM));

        return config;
    }
}
