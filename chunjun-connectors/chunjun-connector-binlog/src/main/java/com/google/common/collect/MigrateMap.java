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
package com.google.common.collect;

import com.google.common.base.Function;

import java.util.concurrent.ConcurrentMap;

/** @author toutian */
public class MigrateMap {

    @SuppressWarnings("deprecation")
    public static <K, V> ConcurrentMap<K, V> makeComputingMap(
            MapMaker maker, Function<? super K, ? extends V> computingFunction) {
        return MapMakerHelper.makeComputingMap(maker, computingFunction);
    }

    @SuppressWarnings("deprecation")
    public static <K, V> ConcurrentMap<K, V> makeComputingMap(
            Function<? super K, ? extends V> computingFunction) {
        return MapMakerHelper.makeComputingMap(new MapMaker(), computingFunction);
    }
}
