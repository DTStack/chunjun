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

package com.dtstack.chunjun.connector.mongodb.table.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class MongoClientOptions {

    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the MongoDB uri.");

    public static final ConfigOption<String> COLLECTION =
            ConfigOptions.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the MongoDB table name.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the MongoDB username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the MongoDB password.");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the MongoDB database.");
    // Read Only
    public static final ConfigOption<String> FILTER =
            ConfigOptions.key("filter").stringType().noDefaultValue().withDescription("");

    public static final ConfigOption<String> FETCH_SIZE =
            ConfigOptions.key("fetch-size").stringType().noDefaultValue().withDescription("");
}
