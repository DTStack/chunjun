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

package com.dtstack.chunjun.connector.cassandra.conf;

import org.apache.flink.configuration.ReadableConfig;

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

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraSinkConf extends CassandraCommonConf {

    private boolean asyncWrite;

    public boolean isAsyncWrite() {
        return asyncWrite;
    }

    public void setAsyncWrite(boolean asyncWrite) {
        this.asyncWrite = asyncWrite;
    }

    public static CassandraSinkConf from(ReadableConfig config) {
        CassandraSinkConf sinkConf = new CassandraSinkConf();

        sinkConf.setHost(config.get(HOST));
        sinkConf.setPort(config.get(PORT));
        sinkConf.setUserName(config.get(USER_NAME));
        sinkConf.setPassword(config.get(PASSWORD));
        sinkConf.setTableName(config.get(TABLE_NAME));
        sinkConf.setKeyspaces(config.get(KEY_SPACES));

        sinkConf.setClusterName(config.get(CLUSTER_NAME));
        sinkConf.setConsistency(config.get(CONSISTENCY));
        sinkConf.setHostDistance(config.get(HOST_DISTANCE));

        sinkConf.setAsyncWrite(config.get(ASYNC_WRITE));
        sinkConf.setConnectTimeoutMillis(config.get(CONNECT_TIMEOUT_MILLISECONDS));
        sinkConf.setCoreConnectionsPerHost(config.get(CORE_CONNECTIONS_PER_HOST));
        sinkConf.setMaxQueueSize(config.get(MAX_QUEUE_SIZE));
        sinkConf.setMaxConnectionsPerHost(config.get(MAX_CONNECTIONS__PER_HOST));
        sinkConf.setMaxRequestsPerConnection(config.get(MAX_REQUESTS_PER_CONNECTION));
        sinkConf.setPoolTimeoutMillis(config.get(POOL_TIMEOUT_MILLISECONDS));
        sinkConf.setReadTimeoutMillis(config.get(READ_TIME_OUT_MILLISECONDS));
        sinkConf.setUseSSL(config.get(USE_SSL));

        return sinkConf;
    }

    @Override
    public String toString() {
        return "CassandraSinkConf{"
                + "host='"
                + host
                + '\''
                + ", port="
                + port
                + ", userName='"
                + userName
                + '\''
                + ", password='"
                + "**********"
                + '\''
                + ", tableName='"
                + tableName
                + '\''
                + ", keyspaces='"
                + keyspaces
                + '\''
                + ", hostDistance='"
                + hostDistance
                + '\''
                + ", useSSL="
                + useSSL
                + ", clusterName='"
                + clusterName
                + '\''
                + ", consistency='"
                + consistency
                + '\''
                + ", coreConnectionsPerHost="
                + coreConnectionsPerHost
                + ", maxConnectionsPerHost="
                + maxConnectionsPerHost
                + ", maxRequestsPerConnection="
                + maxRequestsPerConnection
                + ", maxQueueSize="
                + maxQueueSize
                + ", readTimeoutMillis="
                + readTimeoutMillis
                + ", connectTimeoutMillis="
                + connectTimeoutMillis
                + ", poolTimeoutMillis="
                + poolTimeoutMillis
                + ", asyncWrite="
                + asyncWrite
                + '}';
    }
}
