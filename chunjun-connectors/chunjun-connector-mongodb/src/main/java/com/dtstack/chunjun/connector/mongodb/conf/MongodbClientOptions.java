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

package com.dtstack.chunjun.connector.mongodb.conf;

import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;

import java.io.Serializable;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/27
 */
public class MongodbClientOptions implements Serializable {
    private int connectionsPerHost = 100;

    private int threadsForConnectionMultiplier = 100;

    private int connectionTimeout = 10000;

    private int maxWaitTime = 5000;

    private int socketTimeout = 0;

    public static MongoClientOptions getClientOptionsWhenDataSync(
            MongodbClientOptions mongodbClientOptions) {
        MongoClientOptions.Builder build = new MongoClientOptions.Builder();
        build.connectionsPerHost(mongodbClientOptions.getConnectionsPerHost());
        build.threadsAllowedToBlockForConnectionMultiplier(
                mongodbClientOptions.getThreadsForConnectionMultiplier());
        build.connectTimeout(mongodbClientOptions.getConnectionTimeout());
        build.maxWaitTime(mongodbClientOptions.getMaxWaitTime());
        build.socketTimeout(mongodbClientOptions.getSocketTimeout());
        build.writeConcern(WriteConcern.UNACKNOWLEDGED);
        return build.build();
    }

    public int getConnectionsPerHost() {
        return connectionsPerHost;
    }

    public void setConnectionsPerHost(int connectionsPerHost) {
        this.connectionsPerHost = connectionsPerHost;
    }

    public int getThreadsForConnectionMultiplier() {
        return threadsForConnectionMultiplier;
    }

    public void setThreadsForConnectionMultiplier(int threadsForConnectionMultiplier) {
        this.threadsForConnectionMultiplier = threadsForConnectionMultiplier;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getMaxWaitTime() {
        return maxWaitTime;
    }

    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    @Override
    public String toString() {
        return "ConnectionConfig{"
                + "connectionsPerHost="
                + connectionsPerHost
                + ", threadsForConnectionMultiplier="
                + threadsForConnectionMultiplier
                + ", connectionTimeout="
                + connectionTimeout
                + ", maxWaitTime="
                + maxWaitTime
                + ", socketTimeout="
                + socketTimeout
                + '}';
    }
}
