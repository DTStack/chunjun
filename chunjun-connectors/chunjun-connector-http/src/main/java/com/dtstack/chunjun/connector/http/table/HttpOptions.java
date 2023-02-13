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

package com.dtstack.chunjun.connector.http.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class HttpOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url").stringType().noDefaultValue().withDescription("api url.");

    public static final ConfigOption<String> DECODE =
            ConfigOptions.key("decode")
                    .stringType()
                    .defaultValue("json")
                    .withDescription("decode type");

    public static final ConfigOption<Long> INTERVALTIME =
            ConfigOptions.key("intervalTime").longType().defaultValue(3000L).withDescription("");

    public static final ConfigOption<String> METHOD =
            ConfigOptions.key("method").stringType().defaultValue("post").withDescription("");

    public static final ConfigOption<String> COLUMN =
            ConfigOptions.key("column")
                    .stringType()
                    .defaultValue("[]")
                    .withDescription("return body column");

    public static final ConfigOption<String> HEADER =
            ConfigOptions.key("header")
                    .stringType()
                    .defaultValue("[]")
                    .withDescription("request header");

    public static final ConfigOption<String> BODY =
            ConfigOptions.key("body")
                    .stringType()
                    .defaultValue("[]")
                    .withDescription("request body");

    public static final ConfigOption<String> PARAMS =
            ConfigOptions.key("params")
                    .stringType()
                    .defaultValue("[]")
                    .withDescription("request params");

    public static final ConfigOption<Integer> DELAY =
            ConfigOptions.key("delay").intType().defaultValue(30).withDescription("request delay");

    public static final ConfigOption<String> DATA_SUBJECT =
            ConfigOptions.key("dataSubject")
                    .stringType()
                    .defaultValue("${data}")
                    .withDescription("response data subject");

    public static final ConfigOption<Long> CYCLES =
            ConfigOptions.key("cycles")
                    .longType()
                    .defaultValue(1L)
                    .withDescription("request cycle");
}
