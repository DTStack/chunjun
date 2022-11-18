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

package com.dtstack.chunjun.connector.rocketmq.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class RocketMQOptions {

    // common

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic").stringType().noDefaultValue();

    public static final ConfigOption<String> NAME_SERVER_ADDRESS =
            ConfigOptions.key("nameserver.address").stringType().noDefaultValue();

    public static final ConfigOption<Integer> OPTIONAL_HEART_BEAT_BROKER_INTERVAL =
            ConfigOptions.key("heartbeat.broker.interval").intType().defaultValue(30000);

    public static final ConfigOption<String> OPTIONAL_ACCESS_KEY =
            ConfigOptions.key("access.key").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_SECRET_KEY =
            ConfigOptions.key("secret.key").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_ACCESS_CHANNEL =
            ConfigOptions.key("access.channel").stringType().defaultValue("LOCAL");

    // consumer

    public static final ConfigOption<String> CONSUMER_GROUP =
            ConfigOptions.key("consumer.group").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_TAG =
            ConfigOptions.key("tag").stringType().defaultValue("*");

    public static final ConfigOption<Long> OPTIONAL_START_MESSAGE_OFFSET =
            ConfigOptions.key("start.message-offset").longType().defaultValue(-1L);

    public static final ConfigOption<Long> OPTIONAL_START_MESSAGE_TIMESTAMP =
            ConfigOptions.key("start.message-timestamp").longType().defaultValue(-1L);

    public static final ConfigOption<String> OPTIONAL_START_OFFSET_MODE =
            ConfigOptions.key("consumer.start-offset-mode").stringType().defaultValue("latest");

    public static final ConfigOption<Long> OPTIONAL_START_TIME_MILLS =
            ConfigOptions.key("start.time.ms".toLowerCase()).longType().defaultValue(-1L);

    public static final ConfigOption<String> OPTIONAL_START_TIME =
            ConfigOptions.key("start.time".toLowerCase()).stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_END_TIME =
            ConfigOptions.key("end.time").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_TIME_ZONE =
            ConfigOptions.key("time.zone".toLowerCase()).stringType().defaultValue("GMT+8");

    public static final ConfigOption<Integer> OPTIONAL_PERSIST_CONSUMER_INTERVAL =
            ConfigOptions.key("persist.consumer-offset-interval").intType().defaultValue(5000);

    public static final ConfigOption<String> OPTIONAL_ENCODING =
            ConfigOptions.key("encoding").stringType().defaultValue("UTF-8");

    // public static final ConfigOption<String> OPTIONAL_FIELD_DELIMITER =
    //         ConfigOptions.key("field.delimiter").stringType().defaultValue("\u0001");

    // public static final ConfigOption<String> OPTIONAL_LINE_DELIMITER =
    //         ConfigOptions.key("line.delimiter").stringType().defaultValue("\n");

    public static final ConfigOption<Integer> OPTIONAL_CONSUMER_BATCH_SIZE =
            ConfigOptions.key("consumer.batch-size").intType().defaultValue(32);

    // producer

    public static final ConfigOption<String> PRODUCER_GROUP =
            ConfigOptions.key("producer.group").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> OPTIONAL_COLUMN_ERROR_DEBUG =
            ConfigOptions.key("column.error.debug").booleanType().defaultValue(true);

    public static final ConfigOption<String> OPTIONAL_LENGTH_CHECK =
            ConfigOptions.key("length.check").stringType().defaultValue("NONE");

    public static final ConfigOption<Integer> OPTIONAL_WRITE_RETRY_TIMES =
            ConfigOptions.key("retry.times").intType().defaultValue(10);

    public static final ConfigOption<Long> OPTIONAL_WRITE_SLEEP_TIME_MS =
            ConfigOptions.key("sleep.time.ms").longType().defaultValue(5000L);

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_IS_DYNAMIC_TAG =
            ConfigOptions.key("is.dynamic.tag").booleanType().defaultValue(false);

    public static final ConfigOption<String> OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN =
            ConfigOptions.key("dynamic.tag.column").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN_WRITE_INCLUDED =
            ConfigOptions.key("dynamic.tag.column-write-included").booleanType().defaultValue(true);

    public static final ConfigOption<String> OPTIONAL_WRITE_KEY_COLUMNS =
            ConfigOptions.key("key.columns").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_KEYS_TO_BODY =
            ConfigOptions.key("write.keys.to-body").booleanType().defaultValue(false);
}
