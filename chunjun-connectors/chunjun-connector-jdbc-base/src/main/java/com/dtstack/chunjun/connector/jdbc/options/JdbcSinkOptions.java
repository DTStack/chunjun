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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class JdbcSinkOptions {

    public static final ConfigOption<Boolean> SINK_ALL_REPLACE =
            ConfigOptions.key("sink.all-replace")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("the max retry times if writing records to database failed.");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .defaultValue(null)
                    .withDescription("sink.parallelism.");

    public static final ConfigOption<String> SINK_SEMANTIC =
            ConfigOptions.key("sink.semantic")
                    .stringType()
                    .defaultValue("at-least-once")
                    .withDescription("sink.semantic.");

    public static final ConfigOption<String> SINK_PRE_SQL =
            ConfigOptions.key("sink.pre-sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("sink.pre-sql");

    public static final ConfigOption<String> SINK_POST_SQL =
            ConfigOptions.key("sink.post-sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("sink.post-sql");
}
