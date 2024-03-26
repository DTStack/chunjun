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
package com.dtstack.chunjun.connector.hbase.table;

import com.dtstack.chunjun.table.options.BaseFileOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;

public class HBaseOptions extends BaseFileOptions {
    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of HBase table to connect.");
    public static final ConfigOption<String> ZOOKEEPER_QUORUM =
            ConfigOptions.key("zookeeper.quorum")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The HBase Zookeeper quorum.");
    public static final ConfigOption<String> ZOOKEEPER_ZNODE_PARENT =
            ConfigOptions.key("zookeeper.znode.parent")
                    .stringType()
                    .defaultValue("/hbase")
                    .withDescription("The root dir in Zookeeper for HBase cluster.");
    public static final ConfigOption<String> NULL_STRING_LITERAL =
            ConfigOptions.key("null-string-literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription(
                            "Representation for null values for string fields. HBase source and sink encodes/decodes empty bytes as null values for all types except string type.");

    public static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_SIZE =
            ConfigOptions.key("sink.buffer-flush.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription(
                            "Writing option, maximum size in memory of buffered rows for each writing request. This can improve performance for writing data to HBase database, but may increase the latency. Can be set to '0' to disable it. ");
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Writing option, maximum number of rows to buffer for each writing request. This can improve performance for writing data to HBase database, but may increase the latency. Can be set to '0' to disable it.");
    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1L))
                    .withDescription(
                            "Writing option, the interval to flush any buffered rows. This can improve performance for writing data to HBase database, but may increase the latency. Can be set to '0' to disable it. Note, both 'sink.buffer-flush.max-size' and 'sink.buffer-flush.max-rows' can be set to '0' with the flush interval set allowing for complete async processing of buffered actions.");
    public static final ConfigOption<String> START_ROW_KEY =
            ConfigOptions.key("start-row-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("start row key");
    public static final ConfigOption<String> END_ROW_KEY =
            ConfigOptions.key("end-row-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("end row key");
    public static final ConfigOption<Boolean> IS_BINARY_ROW_KEY =
            ConfigOptions.key("is-binary-row-key")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("is binary row key");
    public static final ConfigOption<Integer> SCAN_CACHE_SIZE =
            ConfigOptions.key("scan-cache-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("scan cache size");
    public static final ConfigOption<String> VERSION_COLUMN_NAME =
            ConfigOptions.key("version-column-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("version column name");
    public static final ConfigOption<String> VERSION_COLUMN_VALUE =
            ConfigOptions.key("version-column-value")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("version column value");
    public static final ConfigOption<String> ROWKEY_EXPRESS =
            ConfigOptions.key("rowkey-express")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("rowkey express");
    public static final ConfigOption<Integer> SCAN_BATCH_SIZE =
            ConfigOptions.key("scan-batch-size")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("scan batch size");

    public static final ConfigOption<Integer> MAX_VERSION =
            ConfigOptions.key("max-version")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("max version");

    public static final ConfigOption<String> MODE =
            ConfigOptions.key("mode")
                    .stringType()
                    .defaultValue("normal")
                    .withDescription("Support normal and multiVersionFixedColumn mode");

    public static final ConfigOption<String> NULL_MODE =
            ConfigOptions.key("null-mode")
                    .stringType()
                    .defaultValue("SKIP")
                    .withDescription("Write mode when field value is empty");
}
