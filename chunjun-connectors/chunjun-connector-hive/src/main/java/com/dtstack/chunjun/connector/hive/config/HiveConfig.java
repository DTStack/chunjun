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
package com.dtstack.chunjun.connector.hive.config;

import com.dtstack.chunjun.connector.hdfs.config.HdfsConfig;
import com.dtstack.chunjun.connector.hive.entity.TableInfo;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class HiveConfig extends HdfsConfig {

    private static final long serialVersionUID = 5161832759671076024L;

    private String jdbcUrl;
    private String username;
    private String password;
    private String partitionType = "DAY";
    private String partition = "pt";
    private String tablesColumn;
    private String distributeTable;
    private String schema;
    private String analyticalRules;

    private Map<String, String> distributeTableMapping = new HashMap<>();
    private Map<String, TableInfo> tableInfos = new HashMap<>();
    private String tableName;
    private boolean autoCreateTable;
}
