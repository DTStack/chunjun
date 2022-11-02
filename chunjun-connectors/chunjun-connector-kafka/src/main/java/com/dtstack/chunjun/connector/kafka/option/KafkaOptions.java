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

package com.dtstack.chunjun.connector.kafka.option;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author chuixue
 * @create 2021-06-07 15:53
 * @description
 */
public class KafkaOptions {
    public static final ConfigOption<String> DEFAULT_CODEC =
            ConfigOptions.key("default.codec")
                    .stringType()
                    .defaultValue("json")
                    .withDescription("default.codec");

    public static final ConfigOption<String> VALUE_CODEC =
            ConfigOptions.key("value.codec")
                    .stringType()
                    .defaultValue("value")
                    .withDescription("value.codec");
}
