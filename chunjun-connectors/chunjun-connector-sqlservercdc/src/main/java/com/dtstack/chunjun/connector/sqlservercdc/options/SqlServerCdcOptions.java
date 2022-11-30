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
package com.dtstack.chunjun.connector.sqlservercdc.options;

import com.dtstack.chunjun.connector.sqlservercdc.format.TimestampFormat;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;

public class SqlServerCdcOptions {

    public static final String SQL = "SQL";
    public static final String ISO_8601 = "ISO-8601";

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SqlServer username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SqlServer password.");

    public static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SqlServer jdbcUrl.");

    public static final ConfigOption<String> CAT =
            ConfigOptions.key("cat")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SqlServer option type.");

    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SqlServer table.");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SqlServer database. ");

    public static final ConfigOption<String> LSN =
            ConfigOptions.key("lsn")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SqlServer lsn.");

    public static final ConfigOption<Long> POLLINTERVAL =
            ConfigOptions.key("poll-interval")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("SqlServer pollInterval.");
    public static final ConfigOption<Boolean> AUTO_COMMIT =
            ConfigOptions.key("auto-commit")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("auto-commit.");

    public static final ConfigOption<Boolean> AUTO_RESET_CONNECTION =
            ConfigOptions.key("auto_reset_connection")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("auto_reset_connection.");

    public static final ConfigOption<String> TIMESTAMP_FORMAT =
            ConfigOptions.key("timestamp-format.standard")
                    .stringType()
                    .defaultValue("SQL")
                    .withDescription(
                            "Optional flag to specify timestamp format, SQL by default."
                                    + " Option ISO-8601 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}\" format and output timestamp in the same format."
                                    + " Option SQL will parse input timestamp in \"yyyy-MM-dd HH:mm:ss.s{precision}\" format and output timestamp in the same format.");

    public static TimestampFormat getTimestampFormat(ReadableConfig config) {
        String timestampFormat = config.get(TIMESTAMP_FORMAT);
        switch (timestampFormat) {
            case SQL:
                return TimestampFormat.SQL;
            case ISO_8601:
                return TimestampFormat.ISO_8601;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
    }
}
