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

package com.dtstack.chunjun.connector.redis.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.regex.Pattern;

public class RedisOptions {
    public static final ConfigOption<Integer> REDIS_CRITICAL_TIME =
            ConfigOptions.key("REDIS_CRITICAL_TIME")
                    .intType()
                    .defaultValue(60 * 60 * 24 * 30)
                    .withDescription("REDIS_CRITICAL_TIME");

    public static final ConfigOption<Integer> REDIS_KEY_VALUE_SIZE =
            ConfigOptions.key("REDIS_KEY_VALUE_SIZE")
                    .intType()
                    .defaultValue(2)
                    .withDescription("REDIS_KEY_VALUE_SIZE");

    public static final ConfigOption<Pattern> REDIS_HOST_PATTERN =
            ConfigOptions.key("redis.host.pattern")
                    .defaultValue(Pattern.compile("(?<host>(.*)):(?<port>\\d+)*"))
                    .withDescription("ip address pattern");

    public static final ConfigOption<String> REDIS_DEFAULT_PORT =
            ConfigOptions.key("redis.default.port")
                    .stringType()
                    .defaultValue("6379")
                    .withDescription("redis.default.port");

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url").stringType().noDefaultValue().withDescription("url");

    public static final ConfigOption<String> TABLENAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("tableName");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue().withDescription("password");

    public static final ConfigOption<Integer> REDISTYPE =
            ConfigOptions.key("redis-type").intType().defaultValue(1).withDescription("redisType");

    public static final ConfigOption<String> MASTERNAME =
            ConfigOptions.key("master-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("masterName");

    public static final ConfigOption<Integer> DATABASE =
            ConfigOptions.key("database").intType().defaultValue(0).withDescription("database");

    public static final ConfigOption<Integer> TIMEOUT =
            ConfigOptions.key("timeout").intType().defaultValue(10000).withDescription("timeout");

    public static final ConfigOption<Integer> MAXTOTAL =
            ConfigOptions.key("max.total").intType().defaultValue(8).withDescription("maxTotal");

    public static final ConfigOption<Integer> MAXIDLE =
            ConfigOptions.key("max.idle").intType().defaultValue(8).withDescription("maxIdle");

    public static final ConfigOption<Integer> MINIDLE =
            ConfigOptions.key("min.idle").intType().defaultValue(0).withDescription("minIdle");

    public static final ConfigOption<Integer> KEYEXPIREDTIME =
            ConfigOptions.key("key.expired-time")
                    .intType()
                    .defaultValue(0)
                    .withDescription("keyExpiredTime");

    public static final ConfigOption<String> REDIS_DATA_TYPE =
            ConfigOptions.key("type").stringType().noDefaultValue().withDescription("type");

    public static final ConfigOption<String> REDIS_DATA_MODE =
            ConfigOptions.key("mode").stringType().noDefaultValue().withDescription("mode");

    public static final ConfigOption<String> KEY_PREFIX =
            ConfigOptions.key("keyPrefix").stringType().noDefaultValue().withDescription("mode");
}
