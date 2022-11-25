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

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class HiveConfig extends HdfsConfig {

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

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getTablesColumn() {
        return tablesColumn;
    }

    public void setTablesColumn(String tablesColumn) {
        this.tablesColumn = tablesColumn;
    }

    public String getDistributeTable() {
        return distributeTable;
    }

    public void setDistributeTable(String distributeTable) {
        this.distributeTable = distributeTable;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getAnalyticalRules() {
        return analyticalRules;
    }

    public void setAnalyticalRules(String analyticalRules) {
        this.analyticalRules = analyticalRules;
    }

    public Map<String, String> getDistributeTableMapping() {
        return distributeTableMapping;
    }

    public void setDistributeTableMapping(Map<String, String> distributeTableMapping) {
        this.distributeTableMapping = distributeTableMapping;
    }

    public Map<String, TableInfo> getTableInfos() {
        return tableInfos;
    }

    public void setTableInfos(Map<String, TableInfo> tableInfos) {
        this.tableInfos = tableInfos;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isAutoCreateTable() {
        return autoCreateTable;
    }

    public void setAutoCreateTable(boolean autoCreateTable) {
        this.autoCreateTable = autoCreateTable;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", HiveConfig.class.getSimpleName() + "[", "]")
                .add("jdbcUrl='" + jdbcUrl + "'")
                .add("username='" + username + "'")
                .add("password='" + password + "'")
                .add("partitionType='" + partitionType + "'")
                .add("partition='" + partition + "'")
                .add("tablesColumn='" + tablesColumn + "'")
                .add("distributeTable='" + distributeTable + "'")
                .add("schema='" + schema + "'")
                .add("analyticalRules='" + analyticalRules + "'")
                .add("distributeTableMapping=" + distributeTableMapping)
                .add("tableInfos=" + tableInfos)
                .add("tableName='" + tableName + "'")
                .add("autoCreateTable=" + autoCreateTable)
                .toString();
    }
}
