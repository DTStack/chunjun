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
        connectorNameMap.put("metadatamysql", new Tuple3<>("mysql", "metaDataMysql", "mysql"));
        connectorNameMap.put("metadatakafka", new Tuple3<>("kafka", "metaDataKafka", "kafka"));
        connectorNameMap.put(
                "metadatavertica", new Tuple3<>("vertica", "metaDataVertica", "vertica"));
        connectorNameMap.put(
                "metadatahbase", new Tuple3<>("hbase14", "metaDataHBase14", "hbase14"));
        connectorNameMap.put("metadatahive2", new Tuple3<>("hive", "metaDataHive", "hive"));
        connectorNameMap.put("metadatahive1", new Tuple3<>("hive1", "metaDataHive1", "hive1"));
        connectorNameMap.put(
                "metadatasparkthrift",
                new Tuple3<>("sparkthrift", "metaDataSparkThrift", "sparkthrift"));
        connectorNameMap.put(
                "metadatasqlserver", new Tuple3<>("sqlserver", "metaDataSqlServer", "sqlserver"));
        connectorNameMap.put("metadataoracle", new Tuple3<>("oracle", "metaDataOracle", "oracle"));
        connectorNameMap.put(
                "metadataphoenix5", new Tuple3<>("phoenix5", "metaDataPhoenix5", "phoenix5"));
        connectorNameMap.put("metadatahive1cdc", new Tuple3<>("hive", "metaDataHiveCdc", "hive"));
        connectorNameMap.put("metadatahive2cdc", new Tuple3<>("hive", "metaDataHiveCdc", "hive"));
        connectorNameMap.put(
                "metadatasparkthriftcdc", new Tuple3<>("hive", "metaDataHiveCdc", "hive"));
        connectorNameMap.put("metadatatidb", new Tuple3<>("tidb", "metaDataTidb", "tidb"));
        connectorNameMap.put("dorisbatch", new Tuple3<>("doris", "doris", "doris"));
        connectorNameMap.put("starrocks", new Tuple3<>("starrocks", "starRocks", null));
        connectorNameMap.put("gbase", new Tuple3<>("gbase", "gBase", null));
        connectorNameMap.put("protobuf", new Tuple3<>("pbformat", "pbformat", null));
        connectorNameMap.put("huaweihbase", new Tuple3<>("huaweihbase", "huaweiHbase", null));
        connectorNameMap.put("huaweihdfs", new Tuple3<>("huaweihdfs", "huaweiHdfs", null));
        connectorNameMap.put("pgwal", new Tuple3<>("pgwal", "PGWal", null));
        connectorNameMap.put("kafka-hw", new Tuple3<>("huaweikafka", "huaweikafka", null));
        connectorNameMap.put("hudi", new Tuple3<>("hudi", "hoodie", null));
        connectorNameMap.put(
                "inceptorhyperbase", new Tuple3<>("inceptorhyperbase", "DirectHyperbase", null));
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
