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

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class TableInfo implements Serializable {
    private static final long serialVersionUID = -6132395514376772629L;

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

    public void addPartition(String partitionField) {
        this.partitionList.add(partitionField);
    }
}
