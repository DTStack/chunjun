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
package com.dtstack.flinkx.metadatahive2.inputformat;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import com.dtstack.flinkx.metadatahive2.constants.Hive2Version;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;

import static com.dtstack.flinkx.metadatahive2.constants.Hive2MetaDataCons.*;

/**
 * @author : tiezhu
 * @date : 2020/3/9
 */
public class Metadatahive2InputFormat extends BaseMetadataInputFormat {

    protected Hive2Version server;

    @Override
    protected List<String> showDatabases(Connection connection) throws SQLException {
        List<String> dbNameList = new ArrayList<>();
        try(Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(SQL_SHOW_DATABASES)) {
            while (rs.next()) {
                dbNameList.add(rs.getString(1));
            }
        }

        return dbNameList;
    }

    @Override
    protected void switchDatabase(String databaseName) throws SQLException {
        statement.get().execute(String.format(SQL_SWITCH_DATABASE, quote(databaseName)));
    }

    @Override
    protected List<String> showTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        try (ResultSet rs = statement.get().executeQuery(SQL_SHOW_TABLES)) {
           int pos = server.tablePosition();
            while (rs.next()) {
                tables.add(rs.getString(pos));
            }
        }

        return tables;
    }

    @Override
    protected String quote(String name) {
        return String.format("`%s`", name);
    }

    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);
        List<Map<String, Object>> columnList = new ArrayList<>();
        List<Map<String, Object>> partitionColumnList = new ArrayList<>();
        Map<String, Object> tableProperties = new HashMap<>(16);

        List<Map<String, String>> metaData = queryData(tableName);
        Iterator<Map<String, String>> it = metaData.iterator();
        int metaDataFlag = 0;
        while(it.hasNext()){
            Map<String, String> lineDataInternal = it.next();
            String colNameInternal = lineDataInternal.get(KEY_COL_NAME);
            if (StringUtils.isBlank(colNameInternal)) {
                continue;
            }
            if(colNameInternal.startsWith("#")){
                switch (colNameInternal){
                    case PARTITION_INFORMATION:
                        metaDataFlag = 1;
                        break;
                    case TABLE_INFORMATION:
                        metaDataFlag = 2;
                        break;
                    default:
                        break;
                }
                continue;
            }
            switch (metaDataFlag){
                case 0:
                    columnList.add(parseColumn(lineDataInternal, result.size()));
                    break;
                case 1:
                    partitionColumnList.add(parseColumn(lineDataInternal, result.size()));
                    break;
                case 2:
                    parseTableProperties(lineDataInternal, tableProperties, it);
                    break;
                default:
                    break;
            }
        }

        if (partitionColumnList.size() > 0) {
            List<String> partitionColumnNames = new ArrayList<>();
            for (Map<String, Object> partitionColumn : partitionColumnList) {
                partitionColumnNames.add(partitionColumn.get(KEY_COLUMN_NAME).toString());
            }

            columnList.removeIf(column -> partitionColumnNames.contains(column.get(KEY_COLUMN_NAME).toString()));
            result.put(KEY_PARTITIONS, showPartitions(tableName));
        }
        result.put(KEY_TABLE_PROPERTIES, tableProperties);
        result.put(KEY_PARTITION_COLUMNS, partitionColumnList);
        result.put(KEY_COLUMN, columnList);

        return result;
    }

    private Map<String, Object> parseColumn(Map<String, String> lineDataInternal, int index){
        String dataTypeInternal = lineDataInternal.get(KEY_COLUMN_DATA_TYPE);
        String commentInternal = lineDataInternal.get(KEY_COLUMN_COMMENT);
        String colNameInternal = lineDataInternal.get(KEY_COL_NAME);

        Map<String, Object> lineResult = new HashMap<>(16);
        lineResult.put(KEY_COLUMN_NAME, colNameInternal);
        lineResult.put(KEY_COLUMN_TYPE, dataTypeInternal);
        lineResult.put(KEY_COLUMN_COMMENT, commentInternal);
        lineResult.put(KEY_COLUMN_INDEX, index);
        return lineResult;
    }

    void parseTableProperties(Map<String, String> lineDataInternal, Map<String, Object> tableProperties, Iterator<Map<String, String>> it){
        String name = lineDataInternal.get(KEY_COL_NAME);

        if (name.contains(KEY_COL_LOCATION)) {
            tableProperties.put(KEY_LOCATION, StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE)));
        }

        if (name.contains(KEY_COL_CREATETIME) || name.contains(KEY_COL_CREATE_TIME)) {
            tableProperties.put(KEY_CREATETIME, StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE)));
        }

        if (name.contains(KEY_COL_LASTACCESSTIME) || name.contains(KEY_COL_LAST_ACCESS_TIME)) {
            tableProperties.put(KEY_LASTACCESSTIME, StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE)));
        }

        if (name.contains(KEY_COL_OUTPUTFORMAT)) {
            String storedClass = lineDataInternal.get(KEY_COLUMN_DATA_TYPE);
            tableProperties.put(KEY_STORED_TYPE, getStoredType(storedClass));
        }

        if (name.contains(KEY_COL_TABLE_PARAMETERS)) {
            String paraFirst = server.tableParaFirstPos()[0];
            String paraSecond = server.tableParaFirstPos()[1];
            while (it.hasNext()) {
                lineDataInternal = it.next();
                String nameInternal = lineDataInternal.get(paraFirst);
                if (null == nameInternal) {
                    continue;
                }

                nameInternal = nameInternal.trim();
                if (nameInternal.contains(KEY_COLUMN_COMMENT)) {
                    tableProperties.put(KEY_COLUMN_COMMENT, StringUtils.trim(lineDataInternal.get(paraSecond)));
                }

                if (nameInternal.contains(KEY_TOTALSIZE)) {
                    tableProperties.put(KEY_TOTALSIZE, StringUtils.trim(lineDataInternal.get(paraSecond)));
                }

                if (nameInternal.contains(KEY_TRANSIENT_LASTDDLTIME)) {
                    tableProperties.put(KEY_TRANSIENT_LASTDDLTIME, StringUtils.trim(lineDataInternal.get(paraSecond)));
                }
            }
        }
    }

    private String getStoredType(String storedClass) {
        if (storedClass.endsWith(TEXT_FORMAT)){
            return TYPE_TEXT;
        } else if (storedClass.endsWith(ORC_FORMAT)){
            return TYPE_ORC;
        } else if (storedClass.endsWith(PARQUET_FORMAT)){
            return TYPE_PARQUET;
        } else {
            return storedClass;
        }
    }


    private List<Map<String, String>> queryData(String table) throws SQLException{
        try (ResultSet rs = statement.get().executeQuery(String.format(SQL_QUERY_DATA, quote(table)))) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> columnNames = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columnNames.add(metaData.getColumnName(i+1));
            }

            List<Map<String, String>> data = new ArrayList<>();
            while (rs.next()) {
                Map<String, String> lineData = new HashMap<>(Math.max((int) (columnCount/.75f) + 1, 16));
                for (String columnName : columnNames) {
                    lineData.put(columnName, rs.getString(columnName));
                }

                data.add(lineData);
            }

            return data;
        }
    }

    private List<Map<String, String>> showPartitions (String table) throws SQLException{
        List<Map<String, String>> partitions = new ArrayList<>();
        try (ResultSet rs = statement.get().executeQuery(String.format(SQL_SHOW_PARTITIONS, quote(table)))) {
            while (rs.next()) {
                String str = rs.getString(1);
                String[] split = str.split(ConstantValue.EQUAL_SYMBOL);
                if(split.length == 2){
                    Map<String, String> map = new LinkedHashMap<>();
                    map.put(KEY_NAME, split[0]);
                    map.put(KEY_VALUE, split[1]);
                    partitions.add(map);
                }
            }
        }

        return partitions;
    }
}
