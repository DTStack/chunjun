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

/** @author liuliu 2022/7/12 */
public class StarRocksSinkOptions {

    public static final ConfigOption<String> SINK_SEMANTIC =
            ConfigOptions.key("semantic")
                    .stringType()
                    .defaultValue(ConstantValue.WRITE_MODE_DEFAULT)
                    .withDescription("exactly once/at least once");

    public static final ConfigOption<Boolean> NAME_MAPPED =
            ConfigOptions.key("name-mapped").booleanType().defaultValue(false).withDescription("");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(ConstantValue.BATCH_SIZE_DEFAULT)
                    .withDescription("");
}
