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

import org.apache.flink.configuration.ReadableConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.ASYNC_WRITE;
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
public class CassandraSinkConfig extends CassandraCommonConfig {

    private static final long serialVersionUID = -8998387564774595834L;

    private boolean asyncWrite;

    public static CassandraSinkConfig from(ReadableConfig config) {
        CassandraSinkConfig sinkConfig = new CassandraSinkConfig();

        sinkConfig.setHost(config.get(HOST));
        sinkConfig.setPort(config.get(PORT));
        sinkConfig.setUserName(config.get(USER_NAME));
        sinkConfig.setPassword(config.get(PASSWORD));
        sinkConfig.setTableName(config.get(TABLE_NAME));
        sinkConfig.setKeyspaces(config.get(KEY_SPACES));

        sinkConfig.setClusterName(config.get(CLUSTER_NAME));
        sinkConfig.setConsistency(config.get(CONSISTENCY));
        sinkConfig.setHostDistance(config.get(HOST_DISTANCE));

        sinkConfig.setAsyncWrite(config.get(ASYNC_WRITE));
        sinkConfig.setConnectTimeoutMillis(config.get(CONNECT_TIMEOUT_MILLISECONDS));
        sinkConfig.setCoreConnectionsPerHost(config.get(CORE_CONNECTIONS_PER_HOST));
        sinkConfig.setMaxQueueSize(config.get(MAX_QUEUE_SIZE));
        sinkConfig.setMaxConnectionsPerHost(config.get(MAX_CONNECTIONS__PER_HOST));
        sinkConfig.setMaxRequestsPerConnection(config.get(MAX_REQUESTS_PER_CONNECTION));
        sinkConfig.setPoolTimeoutMillis(config.get(POOL_TIMEOUT_MILLISECONDS));
        sinkConfig.setReadTimeoutMillis(config.get(READ_TIME_OUT_MILLISECONDS));
        sinkConfig.setUseSSL(config.get(USE_SSL));

        return sinkConfig;
    }
}
