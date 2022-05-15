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
package com.dtstack.chunjun.connector.pgwal.options;

import org.apache.flink.configuration.ConfigOption;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/** */
public class PGWalOptions {

    public static final ConfigOption<String> USERNAME_CONFIG_OPTION =
            key("username").stringType().noDefaultValue();

    public static final ConfigOption<String> PASSWORD_CONFIG_OPTION =
            key("password").stringType().noDefaultValue();

    public static final ConfigOption<String> JDBC_URL_CONFIG_OPTION =
            key("url").stringType().noDefaultValue();

    public static final ConfigOption<String> DATABASE_CONFIG_OPTION =
            key("databaseName").stringType().noDefaultValue();

    public static final ConfigOption<String> CATALOG_CONFIG_OPTION =
            key("cat").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> PAVING_CONFIG_OPTION =
            key("pavingData").booleanType().defaultValue(false);

    public static final ConfigOption<List<String>> TABLES_CONFIG_OPTION =
            key("tableList").stringType().asList().defaultValues();

    public static final ConfigOption<Integer> STATUS_INTERVAL_CONFIG_OPTION =
            key("statusInterval").intType().defaultValue(20000);

    public static final ConfigOption<Long> LSN_CONFIG_OPTION =
            key("lsn").longType().defaultValue(0L);

    public static final ConfigOption<String> SLOT_NAME_CONFIG_OPTION =
            key("slotName").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> ALLOW_CREATE_SLOT_CONFIG_OPTION =
            key("allowCreateSlot").booleanType().defaultValue(true);

    public static final ConfigOption<Boolean> TEMPORARY_CONFIG_OPTION =
            key("temporary").booleanType().defaultValue(true);
}
