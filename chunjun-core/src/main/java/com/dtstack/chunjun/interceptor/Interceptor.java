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

package com.dtstack.chunjun.interceptor;

import org.apache.flink.configuration.Configuration;

import java.io.Closeable;
import java.io.IOException;

public interface Interceptor extends Closeable {

    void init(Configuration configuration);

    void pre(Context context);

    void post(Context context);

    default void close() throws IOException {}

    interface Context extends Iterable<String> {
        void put(String key, Object value);

        int size();

        <T> T get(String key, Class<T> type);

        Object get(String key);
    }
}
