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

package com.dtstack.chunjun.connector.cassandra.config;

import com.dtstack.chunjun.config.CommonConfig;

import org.apache.flink.configuration.ReadableConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CLUSTER_NAME;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CONNECT_TIMEOUT_MILLISECONDS;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CONSISTENCY;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CORE_CONNECTIONS_PER_HOST;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.HOST;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.HOST_DISTANCE;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.KEY_SPACES;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.MAX_CONNECTIONS__PER_HOST;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.MAX_QUEUE_SIZE;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.MAX_REQUESTS_PER_CONNECTION;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.PASSWORD;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.POOL_TIMEOUT_MILLISECONDS;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.PORT;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.READ_TIME_OUT_MILLISECONDS;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.TABLE_NAME;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.USER_NAME;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.USE_SSL;

@EqualsAndHashCode(callSuper = true)
@Data
public class CassandraCommonConfig extends CommonConfig {

    private static final long serialVersionUID = 2026836700071861330L;

    protected String host;

    protected Integer port;

    protected String userName;

    protected String password;

    protected String tableName;

    protected String keyspaces;

    protected String hostDistance = "LOCAL";

    protected boolean useSSL = false;

    protected String clusterName = "chunjun-cluster";

    protected String consistency = "LOCAL_QUORUM";

    protected Integer coreConnectionsPerHost = 8;

    protected Integer maxConnectionsPerHost = 32768;

    protected Integer maxRequestsPerConnection = 1;

    protected Integer maxQueueSize = 10 * 1000;

    protected Integer readTimeoutMillis = 60 * 1000;

    protected Integer connectTimeoutMillis = 60 * 1000;

    protected Integer poolTimeoutMillis = 60 * 1000;

    public static CassandraCommonConfig from(
            ReadableConfig readableConfig, CassandraCommonConfig config) {
        config.setHost(readableConfig.get(HOST));
        config.setPort(readableConfig.get(PORT));
        config.setUserName(readableConfig.get(USER_NAME));
        config.setPassword(readableConfig.get(PASSWORD));
        config.setUseSSL(readableConfig.get(USE_SSL));

        config.setTableName(readableConfig.get(TABLE_NAME));
        config.setKeyspaces(readableConfig.get(KEY_SPACES));

        config.setHostDistance(readableConfig.get(HOST_DISTANCE));
        config.setClusterName(readableConfig.get(CLUSTER_NAME));
        config.setConsistency(readableConfig.get(CONSISTENCY));

        config.setCoreConnectionsPerHost(readableConfig.get(CORE_CONNECTIONS_PER_HOST));
        config.setMaxConnectionsPerHost(readableConfig.get(MAX_CONNECTIONS__PER_HOST));
        config.setMaxQueueSize(readableConfig.get(MAX_QUEUE_SIZE));
        config.setMaxRequestsPerConnection(readableConfig.get(MAX_REQUESTS_PER_CONNECTION));

        config.setReadTimeoutMillis(readableConfig.get(READ_TIME_OUT_MILLISECONDS));
        config.setConnectTimeoutMillis(readableConfig.get(CONNECT_TIMEOUT_MILLISECONDS));
        config.setPoolTimeoutMillis(readableConfig.get(POOL_TIMEOUT_MILLISECONDS));

        return config;
    }
}
