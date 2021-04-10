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

package com.dtstack.flinkx.lookup.options;

import com.dtstack.flinkx.enums.CacheType;

import java.io.Serializable;

/**
 * @author chuixue
 * @create 2021-04-10 13:11
 * @description
 **/
public class LookupOptions implements Serializable {
    /** 表名 */
    private String tableName = "";
    /** 间隔加载时间 */
    private long period = 3600 * 1000L;
    /** 缓存条数 */
    private long cacheSize = 1000L;
    /** 缓存时间 */
    private long cacheTtl = 60 * 1000L;
    /** 缓存类型 */
    private String cache = CacheType.LRU.name();
    /** 失败重试次数 */
    private int maxRetryTimes = 3;
    /** 错误条数 */
    private long errorLimit = Long.MAX_VALUE;
    /** 每批次加载条数 */
    private int fetchSize = 1000;

    public String getTableName() {
        return tableName;
    }

    public LookupOptions setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public long getPeriod() {
        return period;
    }

    public LookupOptions setPeriod(long period) {
        this.period = period;
        return this;
    }

    public String getCache() {
        return cache;
    }

    public LookupOptions setCache(String cache) {
        this.cache = cache;
        return this;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public LookupOptions setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public long getErrorLimit() {
        return errorLimit;
    }

    public LookupOptions setErrorLimit(long errorLimit) {
        this.errorLimit = errorLimit;
        return this;
    }

    public long getCacheTtl() {
        return cacheTtl;
    }

    public LookupOptions setCacheTtl(long cacheTtl) {
        this.cacheTtl = cacheTtl;
        return this;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public LookupOptions setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
        return this;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public LookupOptions setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public static LookupOptions build() {
        return new LookupOptions();
    }

    @Override
    public String toString() {
        return "LookupOptions{" +
                "tableName='" + tableName + '\'' +
                ", period=" + period +
                ", cacheSize=" + cacheSize +
                ", cacheTtl=" + cacheTtl +
                ", cache='" + cache + '\'' +
                ", maxRetryTimes=" + maxRetryTimes +
                ", errorLimit=" + errorLimit +
                ", fetchSize=" + fetchSize +
                '}';
    }
}
