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

package com.dtstack.flinkx.connector.jdbc.constants;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author chuixue
 * @create 2021-04-10 16:19
 * @description
 **/
public class JdbcSinkConstants {
    // write config options
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "the flush max size (includes all append, upsert and delete records), over this number"
                                    + " of records, will flush data. The default value is 100.");

    public static final ConfigOption<Long> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "the flush interval mills, over this time, asynchronous threads will flush data. The "
                                    + "default value is 1s.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the max retry times if writing records to database failed.");

    public static final ConfigOption<Boolean> SINK_ALLREPLACE =
            ConfigOptions.key("sink.allReplace")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("the max retry times if writing records to database failed.");
}
