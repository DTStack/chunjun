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

package com.dtstack.flinkx.classloader;

/**
 * Represents a supplier of results.
 *
 * <p>There is no requirement that a new or distinct result be returned each time the supplier is
 * invoked.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a> whose functional method is
 * {@link #get()}.
 *
 * @param <T> the type of results supplied by this supplier
 * @since 1.8
 * @author toutian
 */
@FunctionalInterface
public interface ClassLoaderSupplier<T> {

    /**
     * 使用给定的类加载器创建对象
     *
     * @param cl 类加载器
     * @return 实例化的对象
     * @throws Exception NoSuchMethodException SecurityException
     */
    T get(ClassLoader cl) throws Exception;
}
