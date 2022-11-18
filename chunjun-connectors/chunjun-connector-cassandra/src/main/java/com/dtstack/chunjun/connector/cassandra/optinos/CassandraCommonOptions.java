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

package com.dtstack.chunjun.connector.cassandra.optinos;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class CassandraCommonOptions {

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Cassandra cluster host.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(9042)
                    .withDescription("Cassandra cluster port. Default 9042.");

    public static final ConfigOption<String> USER_NAME =
            ConfigOptions.key("user-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Cassandra user name");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Cassandra password");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("Cassandra table name");

    public static final ConfigOption<String> KEY_SPACES =
            ConfigOptions.key("keyspaces")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Cassandra key spaces");

    public static final ConfigOption<String> HOST_DISTANCE =
            ConfigOptions.key("hostDistance")
                    .stringType()
                    .defaultValue("local")
                    .withDescription(
                            "Cassandra host distance. Enum 'LOCAL', 'REMOTE', 'IGNORE'. Default 'LOCAL'");

    public static final ConfigOption<String> CLUSTER_NAME =
            ConfigOptions.key("clusterName")
                    .stringType()
                    .defaultValue("chunjun-cluster")
                    .withDescription(
                            "Cassandra params. Use cassandra cluster thread-factory-name.");

    public static final ConfigOption<String> CONSISTENCY =
            ConfigOptions.key("consistency")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Cassandra params. The consistency level for the query.");

    public static final ConfigOption<Boolean> USE_SSL =
            ConfigOptions.key("useSSL")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Cassandra params. Whether use ssl.");

    public static final ConfigOption<Integer> CORE_CONNECTIONS_PER_HOST =
            ConfigOptions.key("coreConnectionsPerHost")
                    .intType()
                    .defaultValue(8)
                    .withDescription("Cassandra params. Core number of connections per host.");

    public static final ConfigOption<Integer> MAX_CONNECTIONS__PER_HOST =
            ConfigOptions.key("maxConnectionsPerHost")
                    .intType()
                    .defaultValue(32768)
                    .withDescription("Cassandra params. Max number of connections per host.");

    public static final ConfigOption<Integer> MAX_REQUESTS_PER_CONNECTION =
            ConfigOptions.key("maxRequestsPerConnection")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Cassandra params. Max number of requests per connection");

    public static final ConfigOption<Integer> MAX_QUEUE_SIZE =
            ConfigOptions.key("maxQueueSize")
                    .intType()
                    .defaultValue(10 * 1000)
                    .withDescription("Cassandra params. Max number of queue size");

    public static final ConfigOption<Integer> READ_TIME_OUT_MILLISECONDS =
            ConfigOptions.key("readTimeoutMills")
                    .intType()
                    .defaultValue(60 * 1000)
                    .withDescription(
                            "Cassandra params. The timeout of read operation. The unit of time is milliseconds.");

    public static final ConfigOption<Integer> CONNECT_TIMEOUT_MILLISECONDS =
            ConfigOptions.key("connectTimeoutMillis")
                    .intType()
                    .defaultValue(60 * 1000)
                    .withDescription(
                            "Cassandra params. The timeout of connection. The unit of time is milliseconds.");

    public static final ConfigOption<Integer> POOL_TIMEOUT_MILLISECONDS =
            ConfigOptions.key("poolTimeoutMillis")
                    .intType()
                    .defaultValue(60 * 1000)
                    .withDescription(
                            "Cassandra params. The timeout of pool operation. The unit of time is milliseconds.");

    public static final ConfigOption<Boolean> ASYNC_WRITE =
            ConfigOptions.key("asyncWrite")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Cassandra params. Async write data to databases.");

    public static final ConfigOption<String> WHERE =
            ConfigOptions.key("where")
                    .stringType()
                    .defaultValue(" 1=1 ")
                    .withDescription("The where clauses of Cassandra select query.");
}
