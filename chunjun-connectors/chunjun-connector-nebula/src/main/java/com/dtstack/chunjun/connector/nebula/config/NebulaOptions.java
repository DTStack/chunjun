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

package com.dtstack.chunjun.connector.nebula.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class NebulaOptions {

    public static final ConfigOption<Integer> CLIENT_CONNECT_TIMEOUT_OPTION =
            ConfigOptions.key("client.connect-timeout")
                    .intType()
                    .defaultValue(1000 * 30)
                    .withDescription("nabula client max connect timeout. default: 30 s");

    public static final ConfigOption<Boolean> IS_NEBULA_RECONECT =
            ConfigOptions.key("nebula.is-reconnect")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("should nebula reconnect when exception throws");

    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("nebula.schema-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("tag or edg's name");

    public static final ConfigOption<Integer> FETCH_SIZE =
            ConfigOptions.key("nebula.fatch-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("the rows of data each fatch action");

    public static final ConfigOption<Integer> BULK_SIZE =
            ConfigOptions.key("nebula.bulk-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("the rows of data each insert action");

    public static final ConfigOption<String> SPACE =
            ConfigOptions.key("nebula.space")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("graph space name");

    public static final ConfigOption<String> SCHEMA_TYPE =
            ConfigOptions.key("nebula.schema-type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("graph schema type: vertex or edge");

    public static final ConfigOption<Boolean> ENABLE_SSL =
            ConfigOptions.key("nebula.enableSSL")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("should enable ssl to connect nebula server");

    public static final ConfigOption<String> SSL_PARAM_TYPE =
            ConfigOptions.key("nebula.sslParamType")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the ssl param type");

    public static final ConfigOption<String> CACRT_FILE_PATH =
            ConfigOptions.key("nebula.caCrtFilePath")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the caCrt File path");

    public static final ConfigOption<String> CRT_FILE_PATH =
            ConfigOptions.key("nebula.crtFilePath")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the crt File path");

    public static final ConfigOption<String> KEY_FILE_PATH =
            ConfigOptions.key("nebula.keyFilePath")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the key File path");

    public static final ConfigOption<String> SSL_PASSWORD =
            ConfigOptions.key("nebula.sslPassword")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the ssl password,only self type of {SSL_PARAM_TYPE} need");

    public static final ConfigOption<Integer> CONNECTION_RETRY =
            ConfigOptions.key("nebula.connection-retry")
                    .intType()
                    .defaultValue(0)
                    .withDescription("the times of retry to connect");

    public static final ConfigOption<Integer> EXECUTION_RETRY =
            ConfigOptions.key("nebula.execution-retry")
                    .intType()
                    .defaultValue(0)
                    .withDescription("the times of retry to execute query");

    public static final ConfigOption<String> STORAGE_ADDRESSES =
            ConfigOptions.key("nebula.storage-addresses")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "the address of nebula storage service: host1:port,host2:port");

    public static final ConfigOption<String> GRAPHD_ADDRESSES =
            ConfigOptions.key("nebula.graphd-addresses")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the address of nebula graphd service: host1:port,host2:port");

    public static final ConfigOption<Integer> READ_TASKS =
            ConfigOptions.key("read.tasks")
                    .intType()
                    .defaultValue(1)
                    .withDescription("the parallelism fo tasks");

    public static final ConfigOption<Integer> WRITE_TASKS =
            ConfigOptions.key("write.tasks")
                    .intType()
                    .defaultValue(1)
                    .withDescription("the parallelism fo tasks");

    public static final ConfigOption<Long> FETCH_INTERVAL =
            ConfigOptions.key("nebula.fetch-interval")
                    .longType()
                    .defaultValue(Long.MAX_VALUE)
                    .withDescription(
                            "Pull data within a given time, and set the fetch-interval to achieve the effect of breakpoint resuming, which is equivalent to dividing the time into multiple pull-up data according to the fetch-interval. default: 30 days");

    public static final ConfigOption<Long> START_TIME =
            ConfigOptions.key("nebula.start-time")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("scan the data after the start-time insert");

    public static final ConfigOption<Long> END_TIME =
            ConfigOptions.key("nebula.end-time")
                    .longType()
                    .defaultValue(Long.MAX_VALUE)
                    .withDescription("scan the data before the end-time insert");

    public static final ConfigOption<Boolean> DEFAULT_ALLOW_PART_SUCCESS =
            ConfigOptions.key("nebula.default-allow-part-success")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if allow part success");

    public static final ConfigOption<Boolean> DEFAULT_ALLOW_READ_FOLLOWER =
            ConfigOptions.key("nebula.default-allow-read-follower")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("if allow read from follower");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("nebula.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the password of nebula");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("nebula.username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the username of nebula");

    public static final ConfigOption<Integer> MIN_CONNS_SIZE =
            ConfigOptions.key("nebula.min.conns.size")
                    .intType()
                    .defaultValue(0)
                    .withDescription("The min connections in nebula pool for all addresses");

    public static final ConfigOption<Integer> MAX_CONNS_SIZE =
            ConfigOptions.key("nebula.max.conns.size")
                    .intType()
                    .defaultValue(10)
                    .withDescription("The max connections in nebula pool for all addresses");

    public static final ConfigOption<Integer> IDLE_TIME =
            ConfigOptions.key("nebula.idle.time")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The idleTime of the connection, unit: millisecond "
                                    + " The connection's idle time more than idleTime, it will be delete "
                                    + " 0 means never delete");

    public static final ConfigOption<Integer> INTERVAL_IDLE =
            ConfigOptions.key("nebula.interval.idle")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "the interval time to check idle connection, unit ms, -1 means no check");

    public static final ConfigOption<Integer> WAIT_IDLE_TIME =
            ConfigOptions.key("nebula.wait.idle.time")
                    .intType()
                    .defaultValue(0)
                    .withDescription("the wait time to get idle connection, unit ms");

    public static final ConfigOption<String> WRITE_MODE =
            ConfigOptions.key("write-mode")
                    .stringType()
                    .defaultValue("insert")
                    .withDescription("write mode : insert/upsert");

    public static final ConfigOption<Integer> FIX_STRING_LEN =
            ConfigOptions.key("nebula.fix-string-len")
                    .intType()
                    .defaultValue(64)
                    .withDescription("nebula fix string data type length");

    public static final ConfigOption<String> VID_TYPE =
            ConfigOptions.key("nebula.vid-type")
                    .stringType()
                    .defaultValue("FIXED_STRING(32)")
                    .withDescription(
                            "the vid type for create space,is space exist,this prop dose not need");
}
