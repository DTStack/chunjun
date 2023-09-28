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

public class StreamLoadOptions {

    public static final ConfigOption<Integer> HTTP_CHECK_TIMEOUT =
            ConfigOptions.key("http.check.timeout")
                    .intType()
                    .defaultValue(ConstantValue.HTTP_CHECK_TIMEOUT_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> QUEUE_OFFER_TIMEOUT =
            ConfigOptions.key("queue.offer.timeout")
                    .intType()
                    .defaultValue(ConstantValue.QUEUE_OFFER_TIMEOUT_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> QUEUE_POLL_TIMEOUT =
            ConfigOptions.key("queue.poll.timeout")
                    .intType()
                    .defaultValue(ConstantValue.QUEUE_POLL_TIMEOUT_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Long> SINK_BATCH_MAX_BYTES =
            ConfigOptions.key("sink.batch.max-bytes")
                    .longType()
                    .defaultValue(ConstantValue.SINK_BATCH_MAX_BYTES_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Long> SINK_BATCH_MAX_ROWS =
            ConfigOptions.key("sink.batch.max-rows")
                    .longType()
                    .defaultValue(ConstantValue.SINK_BATCH_MAX_ROWS_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Map<String, String>> STREAM_LOAD_HEAD_PROPERTIES =
            ConfigOptions.key("stream-load.head.properties")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("");

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
