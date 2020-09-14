/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadataoracle.inputformat;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_DEFAULT;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_NULL;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_SCALE;
import static com.dtstack.flinkx.metadata.MetaDataCons.MAX_TABLE_SIZE;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_COLUMN;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_COLUMN_COMMENT;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_COLUMN_INDEX;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_COLUMN_NAME;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_COLUMN_TYPE;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_CREATE_TIME;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_INDEX_COLUMN_NAME;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_ROWS;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_TABLE_TYPE;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_TOTAL_SIZE;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_COLUMN;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_COLUMN_TOTAL;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_INDEX;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_INDEX_TOTAL;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_TABLE_PROPERTIES_TOTAL;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_SHOW_TABLES;

/**
 * @author : kunni@dtstack.com
 * @date : 2020/6/9
 * @description :Oracle元数据可在系统表中查询
 */

public class MetadataoracleInputFormat extends BaseMetadataInputFormat {

    private static final long serialVersionUID = 1L;

    private Map<String, Map<String, String>> tablePropertiesMap;

    private Map<String, List<Map<String, Object>>> columnListMap;

    private Map<String, List<Map<String, String>>> indexListMap;

    private String allTable;

    private String sql;

    @Override
    protected List<Object> showTables() throws SQLException {
        List<Object> tableNameList = new LinkedList<>();
        String sql = String.format(SQL_SHOW_TABLES, quote(currentDb.get()));
        try (ResultSet rs = statement.get().executeQuery(sql)) {
            while (rs.next()) {
                tableNameList.add(rs.getString(1));
            }
        }
        return tableNameList;
    }

    @Override
    protected void switchDatabase(String databaseName) {
        currentDb.set(databaseName);
    }

    @Override
    protected String quote(String name) {
        return String.format("'%s'",name);
    }

    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);
        Map<String, String> tableProperties = tablePropertiesMap.get(tableName);
        List<Map<String, Object>> columnList = columnListMap.get(tableName);
        List<Map<String, String>> indexList = indexListMap.get(tableName);
        result.put(KEY_TABLE_PROPERTIES, tableProperties);
        result.put(KEY_COLUMN, columnList);
        result.put(KEY_COLUMN_INDEX, indexList);
        return result;
    }

    Map<String, Map<String, String> > queryTableProperties() throws SQLException {
        Map<String, Map<String, String>> tablePropertiesMap = new HashMap<>(16);
        if(StringUtils.isBlank(allTable)){
            sql = String.format(SQL_QUERY_TABLE_PROPERTIES_TOTAL, quote(currentDb.get()));
        }else {
            sql = String.format(SQL_QUERY_TABLE_PROPERTIES, quote(currentDb.get()), allTable);
        }
        try (ResultSet rs = statement.get().executeQuery(sql)) {
            while (rs.next()) {
                Map<String, String> map = new HashMap<>(16);
                map.put(KEY_TOTAL_SIZE, rs.getString(1));
                map.put(KEY_COLUMN_COMMENT, rs.getString(2));
                map.put(KEY_TABLE_TYPE, rs.getString(3));
                map.put(KEY_CREATE_TIME, rs.getString(4));
                map.put(KEY_ROWS, rs.getString(5));
                tablePropertiesMap.put(rs.getString(6), map);
            }
        }
        return tablePropertiesMap;
    }

    Map<String, List<Map<String, String>>> queryIndexList() throws SQLException {
        Map<String, List<Map<String, String>>> indexListMap = new HashMap<>(16);
        if(StringUtils.isBlank(allTable)){
            sql = String.format(SQL_QUERY_INDEX_TOTAL, quote(currentDb.get()));
        }else {
            sql = String.format(SQL_QUERY_INDEX, quote(currentDb.get()), allTable);
        }
        try (ResultSet rs = statement.get().executeQuery(sql)) {
            while (rs.next()) {
                Map<String, String> column = new HashMap<>(16);
                column.put(KEY_COLUMN_NAME, rs.getString(1));
                column.put(KEY_INDEX_COLUMN_NAME, rs.getString(2));
                column.put(KEY_COLUMN_TYPE, rs.getString(3));
                String tableName = rs.getString(4);
                if(indexListMap.containsKey(tableName)){
                    indexListMap.get(tableName).add(column);
                }else {
                    List<Map<String, String>> indexList  = new LinkedList<>();
                    indexList.add(column);
                    indexListMap.put(tableName, indexList);
                }
            }
        }
        return indexListMap;
    }

    Map<String, List<Map<String, Object>>> queryColumnList() throws SQLException {
        Map<String, List<Map<String, Object>>> columnListMap = new HashMap<>(16);
        if(StringUtils.isBlank(allTable)){
            sql = String.format(SQL_QUERY_COLUMN_TOTAL, quote(currentDb.get()));
        }else {
            sql = String.format(SQL_QUERY_COLUMN, quote(currentDb.get()), allTable);
        }
        try (ResultSet rs = statement.get().executeQuery(sql)) {
            while (rs.next()) {
                Map<String, Object> column = new HashMap<>(16);
                column.put(KEY_COLUMN_NAME, rs.getString(1));
                column.put(KEY_COLUMN_TYPE, rs.getString(2));
                column.put(KEY_COLUMN_COMMENT, rs.getString(3));
                String tableName = rs.getString(4);
                column.put(KEY_COLUMN_DEFAULT, rs.getString(5));
                column.put(KEY_COLUMN_NULL, rs.getString(6));
                column.put(KEY_COLUMN_SCALE, rs.getString(7));
                if(columnListMap.containsKey(tableName)){
                    column.put(KEY_COLUMN_INDEX, CollectionUtils.size(columnListMap.get(tableName))+1);
                    columnListMap.get(tableName).add(column);
                }else {
                    List<Map<String, Object>>columnList  = new LinkedList<>();
                    column.put(KEY_COLUMN_INDEX, 1);
                    columnList.add(column);
                    columnListMap.put(tableName, columnList);
                }
            }
        }
        return columnListMap;
    }

    @Override
    protected void init() throws SQLException {
        StringBuilder stringBuilder = new StringBuilder(2 * tableList.size());
        if(tableList.size() <= MAX_TABLE_SIZE){
            for(int index=0;index<tableList.size();index++){
                stringBuilder.append(quote((String) tableList.get(index)));
                if(index!=tableList.size()-1){
                    stringBuilder.append(ConstantValue.COMMA_SYMBOL);
                }
            }
            allTable = stringBuilder.toString();
        }
        tablePropertiesMap = queryTableProperties();
        columnListMap = queryColumnList();
        indexListMap = queryIndexList();
    }
}
