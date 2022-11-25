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

import java.util.StringJoiner;

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

public class CassandraLookupConfig extends LookupConfig {

    private CassandraCommonConfig commonConfig;

    public CassandraCommonConfig getCommonConfig() {
        return commonConfig;
    }

    public void setCommonConfig(CassandraCommonConfig commonConfig) {
        this.commonConfig = commonConfig;
    }

    public static CassandraLookupConfig from(ReadableConfig readableConfig) {
        CassandraLookupConfig conf = new CassandraLookupConfig();

        CassandraCommonConfig kuduCommonConf =
                CassandraCommonConfig.from(readableConfig, new CassandraCommonConfig());
        conf.setCommonConfig(kuduCommonConf);

        // common lookup
        conf.setTableName(readableConfig.get(TABLE_NAME));
        conf.setPeriod(readableConfig.get(LOOKUP_CACHE_PERIOD));
        conf.setCacheSize(readableConfig.get(LOOKUP_CACHE_MAX_ROWS));
        conf.setCacheTtl(readableConfig.get(LOOKUP_CACHE_TTL));
        conf.setCache(readableConfig.get(LOOKUP_CACHE_TYPE));
        conf.setMaxRetryTimes(readableConfig.get(LOOKUP_MAX_RETRIES));
        conf.setErrorLimit(readableConfig.get(LOOKUP_ERROR_LIMIT));
        conf.setFetchSize(readableConfig.get(LOOKUP_FETCH_SIZE));
        conf.setAsyncTimeout(readableConfig.get(LOOKUP_ASYNC_TIMEOUT));
        conf.setParallelism(readableConfig.get(LOOKUP_PARALLELISM));

        return conf;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CassandraLookupConfig.class.getSimpleName() + "[", "]")
                .add("commonConfig=" + commonConfig)
                .add("tableName='" + tableName + "'")
                .add("period=" + period)
                .add("cacheSize=" + cacheSize)
                .add("cacheTtl=" + cacheTtl)
                .add("cache='" + cache + "'")
                .add("maxRetryTimes=" + maxRetryTimes)
                .add("errorLimit=" + errorLimit)
                .add("fetchSize=" + fetchSize)
                .add("asyncTimeout=" + asyncTimeout)
                .add("parallelism=" + parallelism)
                .toString();
    }
}
