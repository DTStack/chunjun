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

package com.dtstack.chunjun.connector.redis.conf;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.redis.enums.RedisConnectType;
import com.dtstack.chunjun.connector.redis.enums.RedisDataMode;
import com.dtstack.chunjun.connector.redis.enums.RedisDataType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chuixue
 * @create 2021-06-16 15:15
 * @description
 */
public class RedisConf extends CommonConfig {
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

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public List<Integer> getKeyIndexes() {
        return keyIndexes;
    }

    public void setKeyIndexes(List<Integer> keyIndexes) {
        this.keyIndexes = keyIndexes;
    }

    public String getKeyFieldDelimiter() {
        return keyFieldDelimiter;
    }

    public void setKeyFieldDelimiter(String keyFieldDelimiter) {
        this.keyFieldDelimiter = keyFieldDelimiter;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public RedisDataType getType() {
        return type;
    }

    public void setType(RedisDataType type) {
        this.type = type;
    }

    public RedisDataMode getMode() {
        return mode;
    }

    public void setMode(RedisDataMode mode) {
        this.mode = mode;
    }

    public String getValueFieldDelimiter() {
        return valueFieldDelimiter;
    }

    public void setValueFieldDelimiter(String valueFieldDelimiter) {
        this.valueFieldDelimiter = valueFieldDelimiter;
    }

    public RedisConnectType getRedisConnectType() {
        return redisConnectType;
    }

    public void setRedisConnectType(RedisConnectType redisConnectType) {
        this.redisConnectType = redisConnectType;
    }

    public String getMasterName() {
        return masterName;
    }

    public void setMasterName(String masterName) {
        this.masterName = masterName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public List<String> getUpdateKey() {
        return updateKey;
    }

    public void setUpdateKey(List<String> updateKey) {
        this.updateKey = updateKey;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public boolean isIndexFillHash() {
        return indexFillHash;
    }

    public void setIndexFillHash(boolean indexFillHash) {
        this.indexFillHash = indexFillHash;
    }

    @Override
    public String toString() {
        return "RedisConf{"
                + "hostPort='"
                + hostPort
                + '\''
                + ", password='"
                + password
                + '\''
                + ", database="
                + database
                + ", keyIndexes="
                + keyIndexes
                + ", keyPrefix="
                + keyPrefix
                + ", indexFillHash="
                + indexFillHash
                + ", keyFieldDelimiter='"
                + keyFieldDelimiter
                + '\''
                + ", dateFormat='"
                + dateFormat
                + '\''
                + ", expireTime="
                + expireTime
                + ", timeout="
                + timeout
                + ", type="
                + type
                + ", mode="
                + mode
                + ", valueFieldDelimiter='"
                + valueFieldDelimiter
                + '\''
                + ", redisConnectType="
                + redisConnectType
                + ", masterName='"
                + masterName
                + '\''
                + ", tableName='"
                + tableName
                + '\''
                + ", maxTotal="
                + maxTotal
                + ", maxIdle="
                + maxIdle
                + ", minIdle="
                + minIdle
                + ", updateKey="
                + updateKey
                + '}';
    }
}
