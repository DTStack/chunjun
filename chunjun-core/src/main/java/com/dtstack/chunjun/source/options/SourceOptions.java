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

package com.dtstack.chunjun.source.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class SourceOptions {
    // read config options
    public static final ConfigOption<String> SCAN_RESTORE_COLUMNNAME =
            ConfigOptions.key("scan.restore.columnname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("restore.columnname.");

    public static final ConfigOption<String> SCAN_RESTORE_COLUMNTYPE =
            ConfigOptions.key("scan.restore.columntype")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("restore.columntype.");

    public static final ConfigOption<String> SCAN_PARTITION_COLUMN =
            ConfigOptions.key("scan.partition.column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the column name used for partitioning the input.");

    public static final ConfigOption<String> SCAN_PARTITION_STRATEGY =
            ConfigOptions.key("scan.partition.strategy")
                    .stringType()
                    .defaultValue("range")
                    .withDescription("the partitionStrategy for the input.");

    public static final ConfigOption<String> SCAN_INCREMENT_COLUMN =
            ConfigOptions.key("scan.increment.column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("scan.increment.column.");

    public static final ConfigOption<String> SCAN_INCREMENT_COLUMN_TYPE =
            ConfigOptions.key("scan.increment.column-type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("scan.increment.column-type.");

    public static final ConfigOption<String> SCAN_ORDER_BY_COLUMN =
            ConfigOptions.key("scan.order-by.column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("scan.order-by.column");

    public static final ConfigOption<Integer> SCAN_POLLING_INTERVAL =
            ConfigOptions.key("scan.polling-interval")
                    .intType()
                    .defaultValue(0)
                    .withDescription("scan.polling-interval");

    public static final ConfigOption<String> SCAN_START_LOCATION =
            ConfigOptions.key("scan.start-location")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("scan.start-location");

    public static final ConfigOption<Integer> SCAN_PARALLELISM =
            ConfigOptions.key("scan.parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("scan parallelism.");

    public static final ConfigOption<Integer> SCAN_QUERY_TIMEOUT =
            ConfigOptions.key("scan.query-timeout")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The new query timeout limit in seconds; zero means there is no limit; Default value 1s");

    public static final ConfigOption<Integer> SCAN_CONNECTION_QUERY_TIMEOUT =
            ConfigOptions.key("scan.connection-timeout")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The new connection timeout limit in seconds; zero means there is no limit; Default value 0s");

    public static final ConfigOption<Integer> SCAN_FETCH_SIZE =
            ConfigOptions.key("scan.fetch-size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "gives the reader a hint as to the number of rows that should be fetched, from"
                                    + " the database when reading per round trip. If the value specified is zero, then the hint is ignored. The"
                                    + " default value is zero.");

    public static final ConfigOption<Integer> SCAN_DEFAULT_FETCH_SIZE =
            ConfigOptions.key("scan.default-fetch-size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "gives the reader a hint as to the number of rows that should be fetched, from"
                                    + " the database when reading per round trip. If the value specified is zero, then the hint is ignored. The"
                                    + " default value is 1024.");
}
