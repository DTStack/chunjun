/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.connector.kudu.options;

import com.dtstack.flinkx.sink.WriteMode;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.SessionConfiguration;

/**
 * @author tiezhu
 * @since 2021/6/9 星期三
 */
public class KuduOptions {

    public static final ConfigOption<String> MASTER_ADDRESS =
            ConfigOptions.key("masters")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kudu master address. Separated by ','");

    public static final ConfigOption<Integer> WORKER_COUNT =
            ConfigOptions.key("workCount")
                    .intType()
                    .defaultValue(2 * Runtime.getRuntime().availableProcessors())
                    .withDescription(
                            "Kudu worker count. Default value is twice the current number of cpu cores");

    public static final ConfigOption<Long> OPERATION_TIMEOUT =
            ConfigOptions.key("operationTimeout")
                    .longType()
                    .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS)
                    .withDescription("Kudu normal operation time out");

    public static final ConfigOption<Long> ADMIN_OPERATION_TIMEOUT =
            ConfigOptions.key("adminOperationTimeout")
                    .longType()
                    .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS)
                    .withDescription("Kudu admin operation time out");

    public static final ConfigOption<Long> QUERY_TIMEOUT =
            ConfigOptions.key("queryTimeout")
                    .longType()
                    .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS)
                    .withDescription(
                            "The timeout for connecting scan token. If not set, it will be the same as operationTimeout");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kudu table name");

    public static final ConfigOption<String> READ_MODE =
            ConfigOptions.key("readMode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kudu scan read mode");

    public static final ConfigOption<String> FILTER_STRING =
            ConfigOptions.key("filterString")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kudu scan filter string");

    public static final ConfigOption<Integer> BATCH_SIZE_BYTES =
            ConfigOptions.key("batchSizeBytes")
                    .intType()
                    .defaultValue(1024 * 1024)
                    .withDescription(
                            "Kudu scan bytes. The maximum number of bytes read at a time, the default is 1MB");

    public static final ConfigOption<Integer> SCAN_PARALLELISM =
            ConfigOptions.key("parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Kudu scan parallelism.");

    public static final ConfigOption<String> FLUSH_MODE =
            ConfigOptions.key("flushMode")
                    .stringType()
                    .defaultValue(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC.name())
                    .withDescription("Kudu flush mode. Default AUTO_FLUSH_SYNC");

    public static final ConfigOption<String> WRITE_MODE =
            ConfigOptions.key("write-mode")
                    .stringType()
                    .defaultValue(WriteMode.APPEND.name())
                    .withDescription("The mode of Kudu record write-operation.");

    public static final ConfigOption<Integer> MAX_BUFFER_SIZE =
            ConfigOptions.key("max-buffer-size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("The max size of Kudu buffer which buffed data.");

    public static final ConfigOption<Integer> FLUSH_INTERVAL =
            ConfigOptions.key("flush-interval")
                    .intType()
                    .defaultValue(10 * 1000)
                    .withDescription(
                            "The interval time of Kudu session flush operator. "
                                    + "It wouldn't take effect while flush-mode set to AUTO_FLUSH_SYNC");

    public static final ConfigOption<Boolean> IGNORE_NOT_FOUND =
            ConfigOptions.key("ignore-not-found")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Ignoring not found operator.");

    public static final ConfigOption<Boolean> IGNORE_DUPLICATE =
            ConfigOptions.key("ignore-duplicate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Ignoring duplicate data.");

    public static final ConfigOption<Long> LIMIT_NUM =
            ConfigOptions.key("limit-num")
                    .longType()
                    .defaultValue(Long.MAX_VALUE)
                    .withDescription("The limit number of kudu lookup source scan for");

    public static final ConfigOption<Boolean> IS_FAULT_TOLERANT =
            ConfigOptions.key("fault-tolerant")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Kudu lookup scan for.");
}
