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
package com.dtstack.chunjun.connector.binlog.options;

import com.dtstack.chunjun.connector.binlog.format.TimestampFormat;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;

public class BinlogOptions {

    public static final String SQL = "SQL";
    public static final String ISO_8601 = "ISO-8601";

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host").stringType().noDefaultValue().withDescription("MySQL host.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port").intType().defaultValue(3306).withDescription("MySQL port.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MySQL username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MySQL password.");

    public static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MySQL jdbcUrl.");

    public static final ConfigOption<String> JOURNAL_NAME =
            ConfigOptions.key("journal-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MySQL Binlog file journal Name.");

    public static final ConfigOption<Long> TIMESTAMP =
            ConfigOptions.key("timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription("MySQL Binlog file timestamp.");

    public static final ConfigOption<Long> POSITION =
            ConfigOptions.key("position")
                    .longType()
                    .noDefaultValue()
                    .withDescription("MySQL Binlog file position.");

    public static final ConfigOption<String> CAT =
            ConfigOptions.key("cat")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MySQL Binlog option type.");

    public static final ConfigOption<String> FILTER =
            ConfigOptions.key("filter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MySQL Binlog filter.");

    public static final ConfigOption<Long> PERIOD =
            ConfigOptions.key("period")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("MySQL Binlog period.");

    public static final ConfigOption<Integer> BUFFER_SIZE =
            ConfigOptions.key("buffer-size")
                    .intType()
                    .defaultValue(256)
                    .withDescription("MySQL Binlog bufferSize.");

    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MySQL Binlog table.");

    public static final ConfigOption<String> CONNECTION_CHARSET =
            ConfigOptions.key("connection-charset")
                    .stringType()
                    .defaultValue("UTF-8")
                    .withDescription("MySQL Binlog connectionCharset.");

    public static final ConfigOption<Boolean> DETECTING_ENABLE =
            ConfigOptions.key("detecting-enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("MySQL Binlog detectingEnable.");

    public static final ConfigOption<String> DETECTING_SQL =
            ConfigOptions.key("detecting-sql")
                    .stringType()
                    .defaultValue("SELECT CURRENT_DATE")
                    .withDescription("MySQL Binlog detectingSQL.");

    public static final ConfigOption<Boolean> ENABLE_TSDB =
            ConfigOptions.key("enable-tsdb")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("MySQL Binlog enableTsdb.");

    public static final ConfigOption<Boolean> PARALLEL =
            ConfigOptions.key("parallel")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("MySQL Binlog parallel.");

    public static final ConfigOption<Integer> PARALLEL_THREAD_SIZE =
            ConfigOptions.key("parallel-thread-size")
                    .intType()
                    .defaultValue(2)
                    .withDescription("MySQL Binlog parallelThreadSize.");

    public static final ConfigOption<Boolean> IS_GTID_MODE =
            ConfigOptions.key("is-gtid-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("MySQL Binlog isGTIDMode.");

    public static final ConfigOption<Integer> QUERY_TIME_OUT =
            ConfigOptions.key("query-time-out")
                    .intType()
                    .defaultValue(300000)
                    .withDescription(
                            "After sending data through the TCP connection (here is the SQL to be executed), the timeout period for waiting for a response, in milliseconds");

    public static final ConfigOption<Integer> CONNECT_TIME_OUT =
            ConfigOptions.key("connect-time-out")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "The timeout period for the database driver (mysql-connector-java) to establish a TCP connection with the mysql server, in milliseconds");
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
