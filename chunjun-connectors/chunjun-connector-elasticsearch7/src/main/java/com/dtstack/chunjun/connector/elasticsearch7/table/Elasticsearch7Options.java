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

package com.dtstack.chunjun.connector.elasticsearch7.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class Elasticsearch7Options {

    public static final ConfigOption<Integer> CLIENT_CONNECT_TIMEOUT_OPTION =
            ConfigOptions.key("client.connect-timeout")
                    .intType()
                    .defaultValue(5000)
                    .withDescription("Elasticsearch client max connect timeout. default: 5000 ms");

    public static final ConfigOption<Integer> CLIENT_SOCKET_TIMEOUT_OPTION =
            ConfigOptions.key("client.socket-timeout")
                    .intType()
                    .defaultValue(1800000)
                    .withDescription(
                            "Elasticsearch client max socket timeout. default: 1800000 ms.");

    public static final ConfigOption<Integer> CLIENT_KEEPALIVE_TIME_OPTION =
            ConfigOptions.key("client.keep-alive-time")
                    .intType()
                    .defaultValue(5000)
                    .withDescription(
                            "Elasticsearch client connection max keepAlive time. default: 5000 ms");

    public static final ConfigOption<Integer> CLIENT_REQUEST_TIMEOUT_OPTION =
            ConfigOptions.key("client.request-timeout")
                    .intType()
                    .defaultValue(2000)
                    .withDescription(
                            "Elasticsearch client connection max request timeout. default:2000 ms");

    public static final ConfigOption<Integer> CLIENT_MAX_CONNECTION_PER_ROUTE_OPTION =
            ConfigOptions.key("client.max-connection-per-route")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Elasticsearch client connection assigns maximum connection per route value. default:10");

    public static final ConfigOption<String> SEARCH_QUERY =
            ConfigOptions.key("search-query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The query parameter of Elasticsearch");

    public static final ConfigOption<String> WRITE_MODE =
            ConfigOptions.key("write-mode")
                    .stringType()
                    .defaultValue("append")
                    .withDescription(
                            "Data cleaning processing mode before elasticsearch writer write:");
}
