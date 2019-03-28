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

package com.dtstack.flinkx.mongodb;

/**
 * Configuration Keys for mongodb plugin
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbConfigKeys {

    public final static String KEY_HOST_PORTS = "hostPorts";

    public final static String KEY_USERNAME = "username";

    public final static String KEY_PASSWORD = "password";

    public final static String KEY_DATABASE = "database";

    public final static String KEY_COLLECTION = "collectionName";

    public final static String KEY_FILTER = "filter";

    public final static String KEY_FETCH_SIZE = "fetchSize";

    public final static String KEY_MODE = "writeMode";

    public final static String KEY_REPLACE_KEY = "replaceKey";

    public final static String KEY_MONGODB_CONFIG = "mongodbConfig";

    public final static String KEY_CONNECTIONS_PERHOST = "connectionsPerHost";

    public final static String KEY_THREADS_FOR_CONNECTION_MULTIPLIER = "threadsForConnectionMultiplier";

    public final static String KEY_CONNECTION_TIMEOUT = "connectionTimeout";

    public final static String KEY_MAX_WAIT_TIME = "maxWaitTime";

    public final static String KEY_SOCKET_TIMEOUT = "socketTimeout";
}
