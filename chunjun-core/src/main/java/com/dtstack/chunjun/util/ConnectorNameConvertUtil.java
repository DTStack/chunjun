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

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: shifang
 * @description convert plugin name
 * @date: 2021/8/10 下午5:58
 */
public class ConnectorNameConvertUtil {

    // tuple f0 package name && directory name,f1 class name
    private static Map<String, Tuple2<String, String>> connectorNameMap = new HashMap<>();

    static {
        connectorNameMap.put("es", new Tuple2<>("elasticsearch6", "elasticsearch6"));
        connectorNameMap.put("hbase", new Tuple2<>("hbase14", "HBase14"));
        connectorNameMap.put("tidb", new Tuple2<>("mysql", "mysql"));
        connectorNameMap.put("restapi", new Tuple2<>("http", "http"));
        connectorNameMap.put("adbpostgresql", new Tuple2<>("postgresql", "postgresql"));
        connectorNameMap.put("dorisbatch", new Tuple2<>("doris", "doris"));
        connectorNameMap.put("gbase", new Tuple2<>("gBase", "gBase"));
        connectorNameMap.put("protobuf", new Tuple2<>("pbformat", "pbformat"));
        connectorNameMap.put("starrocks", new Tuple2<>("starrocks", "starRocks"));
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
}
