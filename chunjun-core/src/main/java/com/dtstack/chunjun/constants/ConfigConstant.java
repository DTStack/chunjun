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
package com.dtstack.chunjun.constants;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static com.dtstack.chunjun.constants.ConstantValue.SHIP_FILE_PLUGIN_LOAD_MODE;

public class ConfigConstant {

    public static final String KEY_COLUMN = "column";

    // ChunJun Restart strategy
    public static final String STRATEGY_NO_RESTART = "NoRestart";

    public static final ConfigOption<String> FLINK_PLUGIN_LOAD_MODE_KEY =
            ConfigOptions.key("pluginLoadMode")
                    .stringType()
                    .defaultValue(SHIP_FILE_PLUGIN_LOAD_MODE)
                    .withDescription(
                            "The config parameter defining YarnPer mode plugin loading method."
                                    + "classpath: The plugin package is not uploaded when the task is submitted. "
                                    + "The plugin package needs to be deployed in the pluginRoot directory of the yarn-node node, but the task starts faster"
                                    + "shipfile: When submitting a task, upload the plugin package under the pluginRoot directory to deploy the plug-in package. "
                                    + "The yarn-node node does not need to deploy the plugin package. "
                                    + "The task startup speed depends on the size of the plugin package and the network environment.");
}
