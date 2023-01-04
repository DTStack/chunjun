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

package com.dtstack.chunjun.connector.jdbc.options;

import com.dtstack.chunjun.connector.jdbc.lookup.provider.DruidDataSourceProvider;
import com.dtstack.chunjun.lookup.options.LookupOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.LinkedHashMap;
import java.util.Map;

public class JdbcLookupOptions extends LookupOptions {

    public static final String DRUID_PREFIX = "druid.";
    public static final String VERTX_PREFIX = "vertx.";

    // vertx config options
    public static final ConfigOption<Integer> VERTX_WORKER_POOL_SIZE =
            ConfigOptions.key("vertx.worker-pool-size")
                    .intType()
                    .defaultValue(5)
                    .withDescription("all lookup type period time.");

    public static final ConfigOption<Integer> MAX_TASK_QUEUE_SIZE =
            ConfigOptions.key("MAX_TASK_QUEUE_SIZE")
                    .intType()
                    .defaultValue(100000)
                    .withDescription(" lookup ");

    public static final ConfigOption<Integer> DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE =
            ConfigOptions.key("DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE")
                    .intType()
                    .defaultValue(1)
                    .withDescription(" lookup ");

    public static final ConfigOption<Integer> DEFAULT_VERTX_WORKER_POOL_SIZE =
            ConfigOptions.key("DEFAULT_VERTX_WORKER_POOL_SIZE")
                    .intType()
                    .defaultValue(Runtime.getRuntime().availableProcessors() * 2)
                    .withDescription(" lookup ");

    public static final ConfigOption<Integer> DEFAULT_DB_CONN_POOL_SIZE =
            ConfigOptions.key("DEFAULT_DB_CONN_POOL_SIZE")
                    .intType()
                    .defaultValue(
                            DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE.defaultValue()
                                    + DEFAULT_VERTX_WORKER_POOL_SIZE.defaultValue())
                    .withDescription(" lookup ");

    public static final ConfigOption<Integer> MAX_DB_CONN_POOL_SIZE_LIMIT =
            ConfigOptions.key("MAX_DB_CONN_POOL_SIZE_LIMIT")
                    .intType()
                    .defaultValue(5)
                    .withDescription(" lookup ");

    public static final ConfigOption<Integer> DEFAULT_IDLE_CONNECTION_TEST_PEROID =
            ConfigOptions.key("DEFAULT_IDLE_CONNECTION_TEST_PEROID")
                    .intType()
                    .defaultValue(60)
                    .withDescription(" lookup ");

    public static final ConfigOption<Boolean> DEFAULT_TEST_CONNECTION_ON_CHECKIN =
            ConfigOptions.key("DEFAULT_TEST_CONNECTION_ON_CHECKIN")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(" lookup ");

    public static final ConfigOption<String> DT_PROVIDER_CLASS =
            ConfigOptions.key("DT_PROVIDER_CLASS")
                    .stringType()
                    .defaultValue(DruidDataSourceProvider.class.getName())
                    .withDescription(" lookup ");

    public static final ConfigOption<String> PREFERRED_TEST_QUERY_SQL =
            ConfigOptions.key("PREFERRED_TEST_QUERY_SQL")
                    .stringType()
                    .defaultValue("SELECT 1 FROM DUAL")
                    .withDescription(" lookup ");

    public static final ConfigOption<Integer> ERRORLOG_PRINTNUM =
            ConfigOptions.key("ERRORLOG_PRINTNUM")
                    .intType()
                    .defaultValue(3)
                    .withDescription(" lookup ");

    public static LinkedHashMap<String, Object> getLibConfMap(
            Map<String, String> tableOptions, String prefix) {
        final LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        if (hasLibProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(prefix))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                map.put(key, value);
                            });
        }
        return map;
    }

    private static boolean hasLibProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(DRUID_PREFIX));
    }
}
