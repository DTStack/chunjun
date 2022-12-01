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

package com.dtstack.chunjun.lookup.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

public class LRUCache extends AbstractSideCache {

    protected transient Cache<String, CacheObj> cache;
    private final Long cacheSize;
    private final Long timeOut;

    public LRUCache(Long cacheSize, Long timeOut) {
        this.cacheSize = cacheSize;
        this.timeOut = timeOut;
    }

    @Override
    public void initCache() {
        // 当前只有LRU
        cache =
                CacheBuilder.newBuilder()
                        .maximumSize(cacheSize)
                        .expireAfterWrite(timeOut, TimeUnit.MILLISECONDS)
                        .build();
    }

    @Override
    public CacheObj getFromCache(String key) {
        if (cache == null) {
            return null;
        }

        return cache.getIfPresent(key);
    }

    @Override
    public void putCache(String key, CacheObj value) {
        if (cache == null) {
            return;
        }

        cache.put(key, value);
    }
}
