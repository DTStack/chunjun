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
package com.dtstack.chunjun.connector.hive.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class TableInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    private List<String> columnNameList;
    private List<String> columnTypeList;
    private String createTableSql;
    private String tableName;
    private String tablePath;
    private String path;
    private String store;
    private String delimiter;
    private List<String> partitionList;

    public TableInfo(int columnSize) {
        columnNameList = new ArrayList<>(columnSize);
        columnTypeList = new ArrayList<>(columnSize);
        partitionList = new ArrayList<>();
    }

    public void addColumnAndType(String columnName, String columnType) {
        columnNameList.add(columnName);
        columnTypeList.add(columnType);
    }

    public List<String> getColumnNameList() {
        return columnNameList;
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }

    public List<String> getColumnTypeList() {
        return columnTypeList;
    }

    public void setColumnTypeList(List<String> columnTypeList) {
        this.columnTypeList = columnTypeList;
    }

    public String getCreateTableSql() {
        return createTableSql;
    }

    public void setCreateTableSql(String createTableSql) {
        this.createTableSql = createTableSql;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTablePath() {
        return tablePath;
    }

    public void setTablePath(String tablePath) {
        this.tablePath = tablePath;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getStore() {
        return store;
    }

    public void setStore(String store) {
        this.store = store;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public List<String> getPartitionList() {
        return partitionList;
    }

    public void setPartitionList(List<String> partitionList) {
        this.partitionList = partitionList;
    }

    public void addPartition(String partitionField) {
        this.partitionList.add(partitionField);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TableInfo.class.getSimpleName() + "[", "]")
                .add("columnNameList=" + columnNameList)
                .add("columnTypeList=" + columnTypeList)
                .add("createTableSql='" + createTableSql + "'")
                .add("tableName='" + tableName + "'")
                .add("tablePath='" + tablePath + "'")
                .add("path='" + path + "'")
                .add("store='" + store + "'")
                .add("delimiter='" + delimiter + "'")
                .add("partitionList=" + partitionList)
                .toString();
    }
}
