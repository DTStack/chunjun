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

package com.dtstack.chunjun.connector.vertica11.lookup.options;

import com.dtstack.chunjun.connector.jdbc.options.JdbcLookupOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class Vertica11LookupOptions extends JdbcLookupOptions {

    // vertx config options
    public static final ConfigOption<Integer> VERTX_WORKER_POOL_SIZE =
            ConfigOptions.key("vertx.worker-pool-size")
                    .intType()
                    .defaultValue(5)
                    .withDescription("all lookup type period time.");

    public static final ConfigOption<String> DT_PROVIDER_CLASS =
            ConfigOptions.key("DT_PROVIDER_CLASS")
                    .stringType()
                    .defaultValue(
                            "com.dtstack.chunjun.connector.vertica11.lookup.provider.Vertica11DataSourceProvider")
                    .withDescription(" lookup ");
}
