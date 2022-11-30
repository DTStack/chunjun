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
package com.dtstack.chunjun.connector.oraclelogminer.options;

import com.dtstack.chunjun.connector.oraclelogminer.format.TimestampFormat;
import com.dtstack.chunjun.constants.ConstantValue;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;

public class LogminerOptions {

    public static final String SQL = "SQL";
    public static final String ISO_8601 = "ISO-8601";

    public static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Oracle jdbcUrl.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Oracle username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Oracle password.");

    public static final ConfigOption<Integer> FETCHSIZE =
            ConfigOptions.key("fetch-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Oracle LogMiner fetchSize.");

    public static final ConfigOption<String> CAT =
            ConfigOptions.key("cat")
                    .stringType()
                    .defaultValue("UPDATE,INSERT,DELETE")
                    .withDescription("Oracle LogMiner option type.");

    public static final ConfigOption<String> POSITION =
            ConfigOptions.key("read-position")
                    .stringType()
                    .defaultValue("current")
                    .withDescription("Oracle LogMiner start type.");

    public static final ConfigOption<Long> START_TIME =
            ConfigOptions.key("start-time")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Oracle LogMiner start TIMESTAMP.");

    public static final ConfigOption<String> START_SCN =
            ConfigOptions.key("start-scn")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Oracle LogMiner start SCN.");

    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Oracle LogMiner table.");

    public static final ConfigOption<Long> QUERY_TIMEOUT =
            ConfigOptions.key("query-timeout")
                    .longType()
                    .defaultValue(300L)
                    .withDescription("Oracle LogMiner queryTimeOut.");

    public static final ConfigOption<Boolean> SUPPORT_AUTO_LOG =
            ConfigOptions.key("support-auto-add-log")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Oracle LogMiner supportAutoAddLog.");

    public static final ConfigOption<Integer> IO_THREADS =
            ConfigOptions.key("io-threads")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Oracle LogMiner load redoLog threads.");

    public static final ConfigOption<Long> MAX_LOAD_FILE_SIZE =
            ConfigOptions.key("max-log-file-size")
                    .longType()
                    .defaultValue(5 * ConstantValue.STORE_SIZE_G)
                    .withDescription("Oracle LogMiner load redoLog size.");

    public static final ConfigOption<Integer> TRANSACTION_CACHE_NUM_SIZE =
            ConfigOptions.key("transaction-cache-num-size")
                    .intType()
                    .defaultValue(800)
                    .withDescription("Oracle LogMiner cache size.");

    public static final ConfigOption<Integer> TRANSACTION_EXPIRE_TIME =
            ConfigOptions.key("transaction-expire-time")
                    .intType()
                    .defaultValue(20)
                    .withDescription(
                            "Oracle LogMiner cache expire time  and  default value is 20 minutes");

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
