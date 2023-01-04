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

package com.dtstack.chunjun.lookup.config;

import com.dtstack.chunjun.enums.CacheType;

import java.io.Serializable;
import java.util.StringJoiner;

public class LookupConfig implements Serializable {

    private static final long serialVersionUID = 4244502617187621548L;

    /** 表名 */
    protected String tableName = "";
    /** 间隔加载时间 */
    protected long period = 3600 * 1000L;
    /** 缓存条数 */
    protected long cacheSize = 1000L;
    /** 缓存时间 */
    protected long cacheTtl = 60 * 1000L;
    /** 缓存类型 */
    protected String cache = CacheType.LRU.name();
    /** 失败重试次数 */
    protected int maxRetryTimes = 3;
    /** 错误条数 */
    protected long errorLimit = Long.MAX_VALUE;
    /** 每批次加载条数 */
    protected int fetchSize = 1000;
    /** 异步超时时长 */
    protected int asyncTimeout = 10000;
    /** 维表并行度 */
    protected Integer parallelism = 1;

    public String getTableName() {
        return tableName;
    }

    public LookupConfig setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public long getPeriod() {
        return period;
    }

    public LookupConfig setPeriod(long period) {
        this.period = period;
        return this;
    }

    public String getCache() {
        return cache;
    }

    public LookupConfig setCache(String cache) {
        this.cache = cache;
        return this;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public LookupConfig setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public long getErrorLimit() {
        return errorLimit;
    }

    public LookupConfig setErrorLimit(long errorLimit) {
        this.errorLimit = errorLimit;
        return this;
    }

    public long getCacheTtl() {
        return cacheTtl;
    }

    public LookupConfig setCacheTtl(long cacheTtl) {
        this.cacheTtl = cacheTtl;
        return this;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public LookupConfig setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
        return this;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public LookupConfig setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public int getAsyncTimeout() {
        return asyncTimeout;
    }

    public LookupConfig setAsyncTimeout(int asyncTimeout) {
        this.asyncTimeout = asyncTimeout;
        return this;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public LookupConfig setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public static LookupConfig build() {
        return new LookupConfig();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", LookupConfig.class.getSimpleName() + "[", "]")
                .add("tableName='" + tableName + "'")
                .add("period=" + period)
                .add("cacheSize=" + cacheSize)
                .add("cacheTtl=" + cacheTtl)
                .add("cache='" + cache + "'")
                .add("maxRetryTimes=" + maxRetryTimes)
                .add("errorLimit=" + errorLimit)
                .add("fetchSize=" + fetchSize)
                .add("asyncTimeout=" + asyncTimeout)
                .add("parallelism=" + parallelism)
                .toString();
    }
}
