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

package com.dtstack.chunjun.connector.doris.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

public class DorisOptions {
    public static final ConfigOption<List<String>> FENODES =
            ConfigOptions.key("feNodes")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("YOUR DORIS FE HOSTNAME AND RESFUL PORT");

    public static final ConfigOption<String> TABLE_IDENTIFY =
            ConfigOptions.key("table.identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("YOUR DORIS DATABASE NAME AND YOUR DORIS TABLE NAME");

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc url of doris.");

    public static final ConfigOption<String> SCHEMA =
            ConfigOptions.key("schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc schema name of doris.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc table name of doris.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("YOUR DORIS CONNECTOR USER NAME");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("YOUR DORIS CONNECTOR  PASSWORD");

    public static final ConfigOption<Integer> REQUEST_TABLET_SIZE =
            ConfigOptions.key("requestTabletSize")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("");

    public static final ConfigOption<Integer> REQUEST_CONNECT_TIMEOUT_MS =
            ConfigOptions.key("requestConnectTimeoutMs")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> REQUEST_READ_TIMEOUT_MS =
            ConfigOptions.key("requestReadTimeoutMs")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> REQUEST_QUERY_TIMEOUT_SEC =
            ConfigOptions.key("requestQueryTimeoutS")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> REQUEST_RETRIES =
            ConfigOptions.key("requestRetries")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_REQUEST_RETRIES_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> REQUEST_BATCH_SIZE =
            ConfigOptions.key("requestBatchSize")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_BATCH_SIZE_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Long> EXEC_MEM_LIMIT =
            ConfigOptions.key("requestBatchSize")
                    .longType()
                    .defaultValue(DorisKeys.DORIS_EXEC_MEM_LIMIT_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> DESERIALIZE_QUEUE_SIZE =
            ConfigOptions.key("deserializeQueueSize")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Boolean> DESERIALIZE_ARROW_ASYNC =
            ConfigOptions.key("deserializeArrowAsync")
                    .booleanType()
                    .defaultValue(DorisKeys.DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("fieldDelimiter")
                    .stringType()
                    .defaultValue(DorisKeys.FIELD_DELIMITER)
                    .withDescription("");

    public static final ConfigOption<String> LINE_DELIMITER =
            ConfigOptions.key("lineDelimiter")
                    .stringType()
                    .defaultValue(DorisKeys.LINE_DELIMITER)
                    .withDescription("");

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("maxRetries")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_MAX_RETRIES_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<String> WRITE_MODE =
            ConfigOptions.key("writeMode")
                    .stringType()
                    .defaultValue(DorisKeys.DORIS_WRITE_MODE_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batchSize")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_BATCH_SIZE_DEFAULT)
                    .withDescription("");
}
