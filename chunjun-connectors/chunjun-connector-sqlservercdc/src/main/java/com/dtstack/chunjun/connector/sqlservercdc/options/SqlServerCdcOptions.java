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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Date: 2021/05/06 Company: www.dtstack.com
 *
 * @author shifang
 */
public class SqlServerCdcOptions {

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
}
