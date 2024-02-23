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
package com.dtstack.chunjun.util;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;
import java.util.Map;

public class ConnectorNameConvertUtil {

    // tuple f0 package name && directory name,f1 class name
    private static final Map<String, Tuple3<String, String, String>> connectorNameMap =
            new HashMap<>();

    static {
        connectorNameMap.put(
                "es", new Tuple3<>("elasticsearch7", "elasticsearch7", "elasticsearch7"));
        connectorNameMap.put("hbase", new Tuple3<>("hbase14", "HBase14", "hbase14"));
        connectorNameMap.put("hbase2", new Tuple3<>("hbase2", "HBase2", null));
        connectorNameMap.put("tidb", new Tuple3<>("mysql", "mysql", "mysql"));
        connectorNameMap.put("restapi", new Tuple3<>("http", "http", "http"));
        connectorNameMap.put(
                "adbpostgresql", new Tuple3<>("postgresql", "postgresql", "postgresql"));

        connectorNameMap.put("dorisbatch", new Tuple3<>("doris", "doris", "doris"));
        connectorNameMap.put("starrocks", new Tuple3<>("starrocks", "starRocks", null));
        connectorNameMap.put("gbase", new Tuple3<>("gbase", "gBase", null));
        connectorNameMap.put("protobuf", new Tuple3<>("pbformat", "pbformat", null));
    }

    public static String convertClassPrefix(String originName) {
        if (!connectorNameMap.containsKey(originName)) {
            return originName;
        }
        return connectorNameMap.get(originName).f1;
    }

    public static String convertPackageName(String originName) {
        if (!connectorNameMap.containsKey(originName)) {
            return originName;
        }
        return connectorNameMap.get(originName).f0;
    }

    public static String convertPluginName(String originName) {
        Tuple3<String, String, String> tuple3 = connectorNameMap.get(originName);
        if (tuple3 == null) {
            return originName;
        }
        return tuple3.f2 != null ? tuple3.f2 : originName;
    }
}
