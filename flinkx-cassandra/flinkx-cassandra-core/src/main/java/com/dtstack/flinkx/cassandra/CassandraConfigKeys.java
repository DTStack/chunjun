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
package com.dtstack.flinkx.cassandra;

/**
 *
 * @Company: www.dtstack.com
 * @author wuhui
 */
public class CassandraConfigKeys {
    /**
     * 未填写，默认是9042
     */
    public final static String KEY_HOST = "host";

    public final static String KEY_PORT = "port";

    public final static String KEY_USERNAME = "username";

    public final static String KEY_PASSWORD = "password";

    public final static String KEY_USE_SSL = "useSSL";

    public final static String KEY_KEY_SPACE = "keyspace";

    public final static String KEY_TABLE = "table";

    public final static String KEY_COLUMN = "column";

    public final static String KEY_WHERE = "where";

    public final static String KEY_ALLOW_FILTERING = "allowFiltering";

    public final static String KEY_CONSITANCY_LEVEL = "consistancyLevel";

    public final static String KEY_ASYNC_WRITE = "asyncWrite";

    public final static String KEY_CONNECTION_PER_HOST = "connectionsPerHost";

    public final static String KEY_MAX_PENDING_CONNECTION = "maxPendingPerConnection";

    /**
     * 异步写入的批次大小，默认1（不异步写入）
     */
    public final static String KEY_BATCH_SIZE = "batchSize";

    public final static String KEY_CASSANDRA_CONFIG = "cassandraConfig";
}
