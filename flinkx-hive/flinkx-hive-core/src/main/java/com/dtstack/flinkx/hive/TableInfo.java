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

package com.dtstack.flinkx.hive;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author toutian
 */
public class TableInfo implements Serializable {

    private List<String> columns;
    private List<String> columnTypes;
    private String createTableSql;
    private String tableName;
    private String tablePath;
    private String path;
    private String store;
    private String delimiter;
    private List<String> partitions;

    public TableInfo(int columnSize) {
        columns = new ArrayList<>(columnSize);
        columnTypes = new ArrayList<>(columnSize);
        partitions = new ArrayList<>();
    }

    public void addColumnAndType(String columnName, String columnType) {
        columns.add(columnName);
        columnTypes.add(columnType);
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(List<String> columnTypes) {
        this.columnTypes = columnTypes;
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

    public List<String> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
    }

    public void addPartition(String partitionField) {
        this.partitions.add(partitionField);
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                ", columns=" + columns +
                ", columnTypes=" + columnTypes +
                ", createTableSql='" + createTableSql + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tablePath='" + tablePath + '\'' +
                ", path='" + path + '\'' +
                ", store='" + store + '\'' +
                ", delimiter='" + delimiter + '\'' +
                ", partitions='" + partitions + '\'' +
                '}';
    }
}
