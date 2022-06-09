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

package com.dtstack.chunjun.lookup.conf;

import com.dtstack.chunjun.enums.CacheType;

import java.io.Serializable;

/**
 * @author chuixue
 * @create 2021-04-10 13:11
 * @description
 */
public class LookupConf implements Serializable {
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

    public LookupConf setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public long getPeriod() {
        return period;
    }

    public LookupConf setPeriod(long period) {
        this.period = period;
        return this;
    }

    public String getCache() {
        return cache;
    }

    public LookupConf setCache(String cache) {
        this.cache = cache;
        return this;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public LookupConf setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public long getErrorLimit() {
        return errorLimit;
    }

    public LookupConf setErrorLimit(long errorLimit) {
        this.errorLimit = errorLimit;
        return this;
    }

    public long getCacheTtl() {
        return cacheTtl;
    }

    public LookupConf setCacheTtl(long cacheTtl) {
        this.cacheTtl = cacheTtl;
        return this;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public LookupConf setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
        return this;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public LookupConf setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public int getAsyncTimeout() {
        return asyncTimeout;
    }

    public LookupConf setAsyncTimeout(int asyncTimeout) {
        this.asyncTimeout = asyncTimeout;
        return this;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public LookupConf setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public static LookupConf build() {
        return new LookupConf();
    }

    @Override
    public String toString() {
        return "LookupConf{"
                + "tableName='"
                + tableName
                + '\''
                + ", period="
                + period
                + ", cacheSize="
                + cacheSize
                + ", cacheTtl="
                + cacheTtl
                + ", cache='"
                + cache
                + '\''
                + ", maxRetryTimes="
                + maxRetryTimes
                + ", errorLimit="
                + errorLimit
                + ", fetchSize="
                + fetchSize
                + ", asyncTimeout="
                + asyncTimeout
                + ", parallelism="
                + parallelism
                + '}';
    }
}
