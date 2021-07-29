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
package com.dtstack.flinkx.launcher;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/10/17
 */
public enum ClassLoaderType {
    NONE, CHILD_FIRST, PARENT_FIRST, CHILD_FIRST_CACHE, PARENT_FIRST_CACHE;

    public static ClassLoaderType getByClassMode(String classMode) {
        ClassLoaderType classLoaderType;
        if ("classpath".equalsIgnoreCase(classMode)) {
            classLoaderType = ClassLoaderType.CHILD_FIRST;
        } else {
            classLoaderType = ClassLoaderType.PARENT_FIRST;
        }

        return classLoaderType;
    }
}
