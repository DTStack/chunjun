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

package com.dtstack.chunjun.lookup.options;

import com.dtstack.chunjun.enums.CacheType;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class LookupOptions {
    // look up config options
    public static final ConfigOption<Long> LOOKUP_CACHE_PERIOD =
            ConfigOptions.key("lookup.cache-period")
                    .longType()
                    .defaultValue(3600 * 1000L)
                    .withDescription("all lookup type period time.");

    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.cache.max-rows")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "the max number of rows of lookup cache, over this value, the oldest rows will "
                                    + "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is "
                                    + "specified. Cache is not enabled as default.");

    public static final ConfigOption<Long> LOOKUP_CACHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .longType()
                    .defaultValue(60 * 1000L)
                    .withDescription("the cache time to live.");

    public static final ConfigOption<String> LOOKUP_CACHE_TYPE =
            ConfigOptions.key("lookup.cache-type")
                    .stringType()
                    .defaultValue(CacheType.LRU.name())
                    .withDescription("lookup type.");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
            ConfigOptions.key("lookup.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the max retry times if lookup database failed.");

    public static final ConfigOption<Long> LOOKUP_ERROR_LIMIT =
            ConfigOptions.key("lookup.error-limit")
                    .longType()
                    .defaultValue(Long.MAX_VALUE)
                    .withDescription("error limit.");

    public static final ConfigOption<Integer> LOOKUP_FETCH_SIZE =
            ConfigOptions.key("lookup.fetch-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("fetch size.");

    public static final ConfigOption<Integer> LOOKUP_ASYNC_TIMEOUT =
            ConfigOptions.key("lookup.async-timeout")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("async timeout.");

    public static final ConfigOption<Integer> LOOKUP_PARALLELISM =
            ConfigOptions.key("lookup.parallelism")
                    .intType()
                    .defaultValue(null)
                    .withDescription("lookup.parallelism.");
}
