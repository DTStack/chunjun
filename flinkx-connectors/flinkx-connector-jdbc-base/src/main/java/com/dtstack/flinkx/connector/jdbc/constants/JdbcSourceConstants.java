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

package com.dtstack.flinkx.connector.jdbc.constants;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author chuixue
 * @create 2021-04-10 16:19
 * @description
 **/
public class JdbcSourceConstants {
    // read config options
    public static final ConfigOption<String> SCAN_PARTITION_COLUMN =
            ConfigOptions.key("scan.partition.column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the column name used for partitioning the input.");
    public static final ConfigOption<Integer> SCAN_PARTITION_NUM =
            ConfigOptions.key("scan.partition.num")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the number of partitions.");
    public static final ConfigOption<Long> SCAN_PARTITION_LOWER_BOUND =
            ConfigOptions.key("scan.partition.lower-bound")
                    .longType()
                    .noDefaultValue()
                    .withDescription("the smallest value of the first partition.");
    public static final ConfigOption<Long> SCAN_PARTITION_UPPER_BOUND =
            ConfigOptions.key("scan.partition.upper-bound")
                    .longType()
                    .noDefaultValue()
                    .withDescription("the largest value of the last partition.");
    public static final ConfigOption<Integer> SCAN_FETCH_SIZE =
            ConfigOptions.key("scan.fetch-size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "gives the reader a hint as to the number of rows that should be fetched, from"
                                    + " the database when reading per round trip. If the value specified is zero, then the hint is ignored. The"
                                    + " default value is zero.");
    public static final ConfigOption<Boolean> SCAN_AUTO_COMMIT =
            ConfigOptions.key("scan.auto-commit")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "sets whether the driver is in auto-commit mode. The default value is true, per"
                                    + " the JDBC spec.");
}
