// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package com.dtstack.chunjun.connector.selectdbcloud.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Options for the Doris connector. */
public class SelectdbcloudOptions {

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("selectdb cluster host.");
    public static final ConfigOption<String> HTTP_PORT =
            ConfigOptions.key("http-port")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("selectdb cluster http port.");

    public static final ConfigOption<String> QUERY_PORT =
            ConfigOptions.key("query-port")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("selectdb cluster query port.");

    public static final ConfigOption<String> CLUSTER_NAME =
            ConfigOptions.key("cluster-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("selectdb cluster name.");

    public static final ConfigOption<String> TABLE_IDENTIFIER =
            ConfigOptions.key("table.identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc table name.");
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc user name.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc password.");
    // Prefix for Doris StreamLoad specific properties.
    public static final String STAGE_LOAD_PROP_PREFIX = "sink.properties.";

    // flink write config options
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.batch.size")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "the flush max size (includes all append, upsert and delete records), over this number"
                                    + " of records, will flush data. The default value is 100.");
    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the max retry times if writing records to database failed.");
    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.batch.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "the flush interval mills, over this time, asynchronous threads will flush data. The "
                                    + "default value is 1s.");
    public static final ConfigOption<Boolean> SINK_ENABLE_DELETE =
            ConfigOptions.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("whether to enable the delete function");
}
