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

import java.sql.SQLException;

public interface ServiceProcessor<T, OUT> extends java.io.Closeable {

    void init(java.util.Map<String, Object> param) throws SQLException;

    DataProcessor<OUT> dataProcessor();

    void process(Context context) throws SQLException;

    interface Context {

        <K, V> V get(K key, Class<V> type);

        <V> V get(Class<V> data);

        <K, V> void set(K key, V data);

        <K> boolean contains(K name);
    }
}
