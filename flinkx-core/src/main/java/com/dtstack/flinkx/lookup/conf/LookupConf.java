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

package com.dtstack.flinkx.lookup.conf;

import com.dtstack.flinkx.enums.CacheType;

import java.io.Serializable;

/**
 * @author chuixue
 * @create 2021-04-09 15:48
 * @description
 **/
public class LookupConf implements Serializable {
    /** 表名 */
    private String tableName = "";
    /** 间隔加载时间 */
    private Long period = 3600 * 1000L;
    /** 缓存条数 */
    private Long cacheSize = 1000L;
    /** 缓存时间 */
    private Long timeOut = 60 * 1000L;
    /** 缓存类型 */
    private String cache = CacheType.LRU.name();
    /** 错误条数 */
    private Long errorLimit = Long.MAX_VALUE;

    public String getTableName() {
        return tableName;
    }

    public LookupConf setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public Long getPeriod() {
        return period;
    }

    public LookupConf setPeriod(Long period) {
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

    public Long getCacheSize() {
        return cacheSize;
    }

    public LookupConf setCacheSize(Long cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public Long getTimeOut() {
        return timeOut;
    }

    public LookupConf setTimeOut(Long timeOut) {
        this.timeOut = timeOut;
        return this;
    }

    public Long getErrorLimit() {
        return errorLimit;
    }

    public LookupConf setErrorLimit(Long errorLimit) {
        this.errorLimit = errorLimit;
        return this;
    }

    public static LookupConf build() {
        return new LookupConf();
    }

    @Override
    public String toString() {
        return "LookupConf{" +
                "tableName='" + tableName + '\'' +
                ", period=" + period +
                ", cacheSize=" + cacheSize +
                ", timeOut=" + timeOut +
                ", cache='" + cache + '\'' +
                ", errorLimit=" + errorLimit +
                '}';
    }
}
