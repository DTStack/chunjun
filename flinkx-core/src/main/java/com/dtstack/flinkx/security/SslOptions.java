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

package com.dtstack.flinkx.security;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** @author dujie */
public class SslOptions {

    public static final ConfigOption<String> KEYSTOREFILENAME =
            ConfigOptions.key("security.ssl-keystore-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the filename of keystore.");

    public static final ConfigOption<String> KEYSTOREPASS =
            ConfigOptions.key("security.ssl-keystore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "the password used to check the integrity of the keystore, the password used to unlock the keystore, or null.");

    public static final ConfigOption<String> TYPE =
            ConfigOptions.key("security.ssl-type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the type of keystore.");
}
