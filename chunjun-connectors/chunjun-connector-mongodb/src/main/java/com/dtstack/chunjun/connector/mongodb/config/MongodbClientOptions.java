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

package com.dtstack.chunjun.connector.mongodb.config;

import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import lombok.Data;

import java.io.Serializable;

@Data
public class MongodbClientOptions implements Serializable {

    private static final long serialVersionUID = 3792530779184681100L;

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
}
