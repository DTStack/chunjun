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
package com.dtstack.flinkx.connector.oraclelogminer.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Date: 2021/05/06
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class LogminerOptions {
    public static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("jdbcUrl")
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
            ConfigOptions.key("fetchSize")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Oracle LogMiner fetchSize.");

    public static final ConfigOption<String> CAT =
            ConfigOptions.key("cat")
                    .stringType()
                    .defaultValue("UPDATE,INSERT,DELETE")
                    .withDescription("Oracle LogMiner option type.");

    public static final ConfigOption<String> POSITION =
            ConfigOptions.key("readPosition")
                    .stringType()
                    .defaultValue("current")
                    .withDescription("Oracle LogMiner start type.");


    public static final ConfigOption<Long> START_TIME =
            ConfigOptions.key("startTime")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Oracle LogMiner start TIMESTAMP.");


    public static final ConfigOption<String> START_SCN =
            ConfigOptions.key("startSCN")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Oracle LogMiner start SCN.");

    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Oracle LogMiner table.");

    public static final ConfigOption<Long> QUERY_TIMEOUT =
            ConfigOptions.key("queryTimeout")
                    .longType()
                    .defaultValue(300L)
                    .withDescription("Oracle LogMiner queryTimeOut.");

    public static final ConfigOption<Boolean> SUPPORT_AUTO_LOG =
            ConfigOptions.key("supportAutoAddLog")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Oracle LogMiner supportAutoAddLog.");
}
