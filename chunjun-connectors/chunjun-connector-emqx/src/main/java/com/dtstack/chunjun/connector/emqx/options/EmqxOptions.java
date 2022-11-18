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

package com.dtstack.chunjun.connector.emqx.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class EmqxOptions {
    public static final ConfigOption<String> BROKER =
            ConfigOptions.key("broker").stringType().noDefaultValue().withDescription(" broker ");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic").stringType().noDefaultValue().withDescription(" topic ");

    public static final ConfigOption<Boolean> ISCLEANSESSION =
            ConfigOptions.key("isCleanSession")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(" isCleanSession ");

    public static final ConfigOption<Integer> QOS =
            ConfigOptions.key("qos").intType().defaultValue(2).withDescription(" qos ");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" username ");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" password ");

    public static final ConfigOption<String> FORMAT =
            ConfigOptions.key("format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription(
                            "Defines the format identifier for encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<String> DEFAULT_CODEC =
            ConfigOptions.key("default.codec")
                    .stringType()
                    .defaultValue("json")
                    .withDescription("default.codec");

    public static final ConfigOption<String> CLIENT_ID_READER =
            ConfigOptions.key("client.id.reader")
                    .stringType()
                    .defaultValue("reader")
                    .withDescription("dclient.id.pre");

    public static final ConfigOption<String> CLIENT_ID_WRITER =
            ConfigOptions.key("client.id.writer")
                    .stringType()
                    .defaultValue("writer")
                    .withDescription("dclient.id.pre");

    /** Number of reconnections * */
    public static final ConfigOption<Integer> connectRetryTimes =
            ConfigOptions.key("connectRetryTimes")
                    .intType()
                    .defaultValue(10)
                    .withDescription(" connectRetryTimes ");
}
