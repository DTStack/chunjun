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

package com.dtstack.chunjun.connector.redis.config;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.redis.enums.RedisConnectType;
import com.dtstack.chunjun.connector.redis.enums.RedisDataMode;
import com.dtstack.chunjun.connector.redis.enums.RedisDataType;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@ToString
public class RedisConfig extends CommonConfig {
    private static final long serialVersionUID = -2964324898543027634L;

    /** ip and port */
    private String hostPort;
    /** password */
    private String password;
    /** database */
    private int database = 0;
    /**
     * keyIndexes indicates which columns at the source end need to be used as keys (the first
     * column starts from 0). If the first column and the second column need to be combined as the
     * key, then the value of keyIndexes is [0,1].
     */
    private List<Integer> keyIndexes = new ArrayList<>();
    /**
     * This configuration item takes into account the situation when there are more than two columns
     * per row of the source data (if your source data has only two columns, namely key and value,
     * you can ignore this configuration item and do not need to fill in). When the value type is
     * string, the value is The separator between the two, such as value1\u0001value2\u0001value3.
     */
    private String keyFieldDelimiter = "\\u0001";

    /** key prefix */
    private String keyPrefix;

    /** In hash type, whether to write the key field into the hash */
    private boolean indexFillHash = true;

    /**
     * When writing to Redis, the time format of Date: "yyyy-MM-dd HH:mm:ss".Write the date as long
     */
    private String dateFormat;
    /**
     * Redis value cache invalidation time (if you need to be permanently valid, you can leave this
     * configuration item unfilled)
     */
    private long expireTime = 0L;
    /** The timeout period for writing to Redis. */
    private int timeout = 3000;
    /** Data type of redis database */
    private RedisDataType type;
    /** Operation type of redis database */
    private RedisDataMode mode;
    /**
     * This configuration item takes into account the situation when there are more than two columns
     * per row of the source data (if your source data has only two columns, namely key and value,
     * you can ignore this configuration item and do not need to fill in). When the value type is
     * string, the value is The separator between the two, such as value1\u0001value2\u0001value3.
     */
    private String valueFieldDelimiter = "\\u0001";
    /** redis mode (1 stand-alone, 2 sentries, 3 clusters) */
    private RedisConnectType redisConnectType = RedisConnectType.STANDALONE;
    /** Master node name (required in sentinel mode) */
    private String masterName;
    /** redis table name */
    private String tableName;
    /** Maximum number of connections */
    private int maxTotal = 8;
    /** Maximum number of idle connections */
    private int maxIdle = 8;
    /** Minimum number of idle connections */
    private int minIdle;
    /** primary key */
    private List<String> updateKey;
}
