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

package com.dtstack.chunjun.connector.elasticsearch5.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/28 19:29
 */
public class DtElasticsearchOptions {

    public static final ConfigOption<String> CLUSTER_OPTION =
            ConfigOptions.key("cluster")
                    .stringType()
                    .defaultValue("elasticsearch")
                    .withDescription("Elasticsearch cluster name to connect to.");

    public static final ConfigOption<Integer> ACTION_TIMEOUT_OPTION =
            ConfigOptions.key("action-timeout")
                    .intType()
                    .defaultValue(5000)
                    .withDescription("timeout when interaction with es.");
}
