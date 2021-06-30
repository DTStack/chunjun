/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.flinkx.connector.pgwal.options;

import org.apache.flink.configuration.ConfigOption;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 *
 */
public class PGWalOptions {

    public static final ConfigOption<String> USERNAME_CONFIG_OPTION =
            key("username")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> PASSWORD_CONFIG_OPTION =
            key("password")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> JDBC_URL_CONFIG_OPTION =
            key("jdbcUrl")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> HOST_CONFIG_OPTION =
            key("host")
                    .stringType()
                    .defaultValue("localhost");

    public static final ConfigOption<String> TABLE_CONFIG_OPTION =
            key("table")
                    .stringType()
                    .defaultValue("");

    public static final ConfigOption<Integer> PORT_CONFIG_OPTION =
            key("port")
                    .intType()
                    .defaultValue(5432);

    public static final ConfigOption<String> CATALOG_CONFIG_OPTION =
            key("cat")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<Boolean> PAVING_CONFIG_OPTION =
            key("pavingData")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<List<String>> TABLES_CONFIG_OPTION =
            key("tableList")
                    .stringType()
                    .asList()
                    .defaultValues();

}
