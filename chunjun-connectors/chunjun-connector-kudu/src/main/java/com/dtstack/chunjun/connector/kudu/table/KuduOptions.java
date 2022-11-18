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

package com.dtstack.chunjun.connector.kudu.table;

import com.dtstack.chunjun.sink.WriteMode;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.SessionConfiguration;

public class KuduOptions {

    public static final ConfigOption<String> MASTER_ADDRESS =
            ConfigOptions.key("masters")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kudu master address. Separated by ','");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kudu table name");

    public static final ConfigOption<Integer> WORKER_COUNT =
            ConfigOptions.key("client.worker-count")
                    .intType()
                    .defaultValue(2 * Runtime.getRuntime().availableProcessors())
                    .withDescription(
                            "Kudu worker count. Default value is twice the current number of cpu cores");

    public static final ConfigOption<Long> OPERATION_TIMEOUT =
            ConfigOptions.key("client.default-operation-timeout-ms")
                    .longType()
                    .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS)
                    .withDescription("Kudu normal operation time out");

    public static final ConfigOption<Long> ADMIN_OPERATION_TIMEOUT =
            ConfigOptions.key("client.default-admin-operation-timeout-ms")
                    .longType()
                    .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS)
                    .withDescription("Kudu admin operation time out");

    public static final ConfigOption<String> FLUSH_MODE =
            ConfigOptions.key("session.flush-mode")
                    .stringType()
                    .defaultValue(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC.name())
                    .withDescription("Kudu flush mode. Default AUTO_FLUSH_SYNC");

    public static final ConfigOption<Integer> MUTATION_BUFFER_SPACE =
            ConfigOptions.key("session.mutation-buffer-space")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("The max size of Kudu buffer which buffed data.");

    public static final ConfigOption<Long> QUERY_TIMEOUT =
            ConfigOptions.key("scan-token.query-timeout")
                    .longType()
                    .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS)
                    .withDescription(
                            "The timeout for connecting scan token. If not set, it will be the same as operationTimeout");

    public static final ConfigOption<String> READ_MODE =
            ConfigOptions.key("scan-token.read-mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kudu scan read mode");

    public static final ConfigOption<Integer> SCAN_BATCH_SIZE_BYTES =
            ConfigOptions.key("scan-token.batch-size-bytes")
                    .intType()
                    .defaultValue(1024 * 1024)
                    .withDescription(
                            "Kudu scan bytes. The maximum number of bytes read at a time, the default is 1MB");

    public static final ConfigOption<String> FILTER_EXPRESSION =
            ConfigOptions.key("filter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kudu scan filter expressions");

    public static final ConfigOption<Integer> SCANNER_BATCH_SIZE_BYTES =
            ConfigOptions.key("scanner.batch-size-bytes")
                    .intType()
                    .defaultValue(1024 * 1024)
                    .withDescription(
                            "Kudu scan bytes. The maximum number of bytes read at a time, the default is 1MB");

    public static final ConfigOption<Long> LIMIT_NUM =
            ConfigOptions.key("scanner.limit")
                    .longType()
                    .defaultValue(Long.MAX_VALUE)
                    .withDescription("The limit number of kudu lookup source scan for");

    public static final ConfigOption<Boolean> FAULT_TOLERANT =
            ConfigOptions.key("scanner.fault-tolerant")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Kudu lookup scan for.");

    public static final ConfigOption<String> WRITE_MODE =
            ConfigOptions.key("sink.write-mode")
                    .stringType()
                    .defaultValue(WriteMode.INSERT.name())
                    .withDescription("The mode of Kudu record write-operation.");
}
