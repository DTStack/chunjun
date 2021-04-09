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

package com.dtstack.flinkx.constants;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * @author chuixue
 * @create 2021-04-09 13:31
 * @description
 **/
public class LookupConstant {
    public static final ConfigOption<String> CACHE =
            key("cache")
                    .stringType()
                    .defaultValue("LRU")
                    .withDescription("lookup table cache type");

    public static final ConfigOption<Integer> CACHE_SIZE =
            key("cacheSize")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("lookup table cache size");
}
