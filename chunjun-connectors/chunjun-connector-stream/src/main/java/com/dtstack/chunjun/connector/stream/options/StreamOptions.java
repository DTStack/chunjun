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

package com.dtstack.chunjun.connector.stream.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static org.apache.flink.configuration.ConfigOptions.key;

public class StreamOptions {
    public static final ConfigOption<Boolean> PRINT =
            key("print").booleanType().defaultValue(true).withDescription("if print .");

    public static final ConfigOption<Long> NUMBER_OF_ROWS =
            key("number-of-rows")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            "Total number of rows to emit. By default, the source is unbounded.");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .defaultValue(null)
                    .withDescription("sink.parallelism.");

    public static final ConfigOption<Long> ROWS_PER_SECOND =
            key("rows-per-second").longType().defaultValue(0L).withDescription("rows-per-second.");
}
