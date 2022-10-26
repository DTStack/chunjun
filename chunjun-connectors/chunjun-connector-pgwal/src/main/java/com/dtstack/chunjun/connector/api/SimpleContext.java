/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.api;

import java.util.HashMap;
import java.util.Map;

public class SimpleContext implements ServiceProcessor.Context {

    private Map<Object, Object> params = new HashMap<>();

    @Override
    public <K, V> V get(K key, Class<V> type) {
        Object object = params.get(key);
        assert type.isAssignableFrom(object.getClass());
        return (V) object;
    }

    @Override
    public <V> V get(Class<V> data) {
        return (V)
                params.values().stream()
                        .filter(value -> data.isAssignableFrom(value.getClass()))
                        .findAny()
                        .get();
    }

    @Override
    public <K, V> void set(K key, V data) {
        params.put(key, data);
    }

    @Override
    public <K> boolean contains(K name) {
        return params.containsKey(name);
    }
}
