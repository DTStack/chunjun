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

package com.dtstack.chunjun.connector.starrocks.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.HashMap;
import java.util.Map;

public class StarRocksSourceOptions {

    public static final ConfigOption<String> FILTER_STATEMENT =
            ConfigOptions.key("filter-statement").stringType().defaultValue("").withDescription("");

    public static final ConfigOption<Integer> SCAN_BE_CLIENT_TIMEOUT =
            ConfigOptions.key("scan.be.client.timeout")
                    .intType()
                    .defaultValue(3000)
                    .withDescription("");

    public static final ConfigOption<Integer> SCAN_BE_CLIENT_KEEP_LIVE_MIN =
            ConfigOptions.key("scan.be.client.keep-live-min")
                    .intType()
                    .defaultValue(10)
                    .withDescription("");

    public static final ConfigOption<Integer> SCAN_BE_QUERY_TIMEOUT_S =
            ConfigOptions.key("scan.be.query.timeout-s")
                    .intType()
                    .defaultValue(600)
                    .withDescription("");

    public static final ConfigOption<Integer> SCAN_BE_FETCH_ROWS =
            ConfigOptions.key("scan.be.fetch-rows")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("");

    public static final ConfigOption<Long> SCAN_BE_FETCH_BYTES_LIMIT =
            ConfigOptions.key("scan.be.fetch-bytes-limit")
                    .longType()
                    .defaultValue(1024 * 1024 * 1014L)
                    .withDescription("");

    public static final ConfigOption<Map<String, String>> SCAN_BE_PARAM_PROPERTIES =
            ConfigOptions.key("scan.be.param.properties")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("");
}
