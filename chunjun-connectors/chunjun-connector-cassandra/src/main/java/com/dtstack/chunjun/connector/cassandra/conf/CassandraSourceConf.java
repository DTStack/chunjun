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

import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.WHERE;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraSourceConf extends CassandraCommonConf {
    private String where;

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public static CassandraSourceConf from(ReadableConfig readableConfig) {
        CassandraSourceConf conf =
                (CassandraSourceConf)
                        CassandraCommonConf.from(readableConfig, new CassandraSourceConf());

        conf.setWhere(readableConfig.get(WHERE));

        return conf;
    }

    @Override
    public String toString() {
        return "CassandraSourceConf{"
                + "host='"
                + host
                + '\''
                + ", port="
                + port
                + ", userName='"
                + userName
                + '\''
                + ", password='"
                + "*********"
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
                + ", where='"
                + where
                + '\''
                + '}';
    }
}
