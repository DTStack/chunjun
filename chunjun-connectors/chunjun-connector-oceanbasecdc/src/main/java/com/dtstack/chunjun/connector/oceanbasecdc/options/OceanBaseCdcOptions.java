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

package com.dtstack.chunjun.connector.oceanbasecdc.options;

import com.dtstack.chunjun.connector.oceanbasecdc.format.TimestampFormat;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;

public class OceanBaseCdcOptions {

    public static final String SQL = "SQL";
    public static final String ISO_8601 = "ISO-8601";

    public static final ConfigOption<String> LOG_PROXY_HOST =
            ConfigOptions.key("log-proxy-host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname or IP address of OceanBase log proxy service.");

    public static final ConfigOption<Integer> LOG_PROXY_PORT =
            ConfigOptions.key("log-proxy-port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Port number of OceanBase log proxy service.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username to be used when connecting to OceanBase.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to be used when connecting to OceanBase.");

    public static final ConfigOption<String> TABLE_WHITELIST =
            ConfigOptions.key("table-whitelist")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table whitelist in format 'tenant'.'db'.'table', will use 'fnmatch' to match the three parts of it, multiple values should be separated by '|'.");

    public static final ConfigOption<Long> STARTUP_TIMESTAMP =
            ConfigOptions.key("start-timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Startup timestamp in seconds.");

    public static final ConfigOption<String> RS_LIST =
            ConfigOptions.key("rootservice-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The semicolon-separated list of root servers in format `ip:rpc_port:sql_port`, corresponding to the parameter 'rootservice_list' in the OceanBase CE.");

    public static final ConfigOption<String> CONFIG_URL =
            ConfigOptions.key("config-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The url used to get root servers list, corresponding to the parameter 'obconfig_url' in the OceanBase EE.");

    public static final ConfigOption<String> TIMEZONE =
            ConfigOptions.key("timezone")
                    .stringType()
                    .defaultValue("+08:00")
                    .withDescription("The timezone in database server.");

    public static final ConfigOption<String> WORKING_MODE =
            ConfigOptions.key("working-mode")
                    .stringType()
                    .defaultValue("memory")
                    .withDescription(
                            "The working mode of 'obcdc', can be `memory` or `storage` (supported from `obcdc` 3.1.3).");

    public static final ConfigOption<String> CAT =
            ConfigOptions.key("cat")
                    .stringType()
                    .defaultValue("insert, delete, update")
                    .withDescription(
                            "Operation types need to be captured, can be one or more of 'insert' 'delete' and 'update', separated by commas.");

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
