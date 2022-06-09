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

package com.dtstack.chunjun.util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class DdlConventNameConvertUtil {

    // tuple f0 package name && directory name,f1 class name
    private static Map<String, Tuple2<String, String>> connectorNameMap = new HashMap<>();

    static {
        connectorNameMap.put("binlog", new Tuple2<>("mysql", "mysql"));
        connectorNameMap.put("sqlserverecdc", new Tuple2<>("sqlservere", "sqlservere"));
        connectorNameMap.put("oraclelogminer", new Tuple2<>("oracle", "oracle"));
    }

    public static String convertPackageName(String originName) {
        if (!connectorNameMap.containsKey(originName)) {
            return originName;
        }
        return connectorNameMap.get(originName).f0;
    }
}
