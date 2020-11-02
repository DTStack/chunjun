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
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_PRIMARY;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_SCALE;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_FALSE;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_INDEX_NAME;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_COMMENT;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_CREATE_TIME;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_ROWS;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_TOTAL_SIZE;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TRUE;
import static com.dtstack.flinkx.metadata.MetaDataCons.MAX_TABLE_SIZE;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_COLUMN;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_COLUMN_COMMENT;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_COLUMN_INDEX;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_COLUMN_NAME;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_COLUMN_TYPE;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_TABLE_TYPE;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_COLUMN;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_COLUMN_TOTAL;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_INDEX;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_INDEX_TOTAL;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_PRIMARY_KEY;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_PRIMARY_KEY_TOTAL;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_TABLE_CREATE_TIME;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_TABLE_CREATE_TIME_TOTAL;
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
    /**
     * 表基本属性
     */
    private Map<String, Map<String, String>> tablePropertiesMap;

    /**
     * 列基本属性
     */
    private Map<String, List<Map<String, Object>>> columnListMap;

    /**
     * 索引基本属性
     */
    private Map<String, List<Map<String, String>>> indexListMap;

    /**
     * 主键信息
     */
    private Map<String, String> primaryKeyMap;

    /**
     * 表创建时间
     */
    private Map<String, String> createdTimeMap;

    private String allTable;

    private String sql;

    @Override
    protected List<Object> showTables() throws SQLException {
        List<Object> tableNameList = new LinkedList<>();
        sql = String.format(SQL_SHOW_TABLES, quote(currentDb.get()));
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
    protected Map<String, Object> queryMetaData(String tableName) {
        Map<String, Object> result = new HashMap<>(16);
        Map<String, String> tableProperties = tablePropertiesMap.get(tableName);
        tableProperties.put(KEY_TABLE_CREATE_TIME, createdTimeMap.get(tableName));
        List<Map<String, Object>> columnList = columnListMap.get(tableName);
        List<Map<String, String>> indexList = indexListMap.get(tableName);
        String primaryColumn = primaryKeyMap.get(tableName);
        for(Map<String, Object> map : columnList){
            if(StringUtils.equals((String) map.get(KEY_COLUMN_NAME), primaryColumn)){
                map.put(KEY_COLUMN_PRIMARY, KEY_TRUE);
            }else{
                map.put(KEY_COLUMN_PRIMARY, KEY_FALSE);
            }
        }
        result.put(KEY_TABLE_PROPERTIES, tableProperties);
        result.put(KEY_COLUMN, columnList);
        result.put(KEY_COLUMN_INDEX, indexList);
        return result;
    }

    protected Map<String, Map<String, String> > queryTableProperties() throws SQLException {
        Map<String, Map<String, String>> tablePropertiesMap = new HashMap<>(16);
        sql = String.format(SQL_QUERY_TABLE_PROPERTIES_TOTAL, quote(currentDb.get()));
        if(StringUtils.isNotBlank(allTable)){
            sql += String.format(SQL_QUERY_TABLE_PROPERTIES, allTable);
        }
        try (ResultSet rs = statement.get().executeQuery(sql)) {
            while (rs.next()) {
                Map<String, String> map = new HashMap<>(16);
                map.put(KEY_TABLE_TOTAL_SIZE, rs.getString(1));
                map.put(KEY_TABLE_COMMENT, rs.getString(2));
                map.put(KEY_TABLE_TYPE, rs.getString(3));
                map.put(KEY_TABLE_ROWS, rs.getString(4));
                tablePropertiesMap.put(rs.getString(5), map);
            }
        }
        return tablePropertiesMap;
    }

    protected Map<String, List<Map<String, String>>> queryIndexList() throws SQLException {
        Map<String, List<Map<String, String>>> indexListMap = new HashMap<>(16);
        sql = String.format(SQL_QUERY_INDEX_TOTAL, quote(currentDb.get()));
        if(StringUtils.isNotBlank(allTable)){
            sql += String.format(SQL_QUERY_INDEX, allTable);
        }
        try (ResultSet rs = statement.get().executeQuery(sql)) {
            while (rs.next()) {
                Map<String, String> column = new HashMap<>(16);
                column.put(KEY_INDEX_NAME, rs.getString(1));
                column.put(KEY_COLUMN_NAME, rs.getString(2));
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

    protected Map<String, List<Map<String, Object>>> queryColumnList() throws SQLException {
        Map<String, List<Map<String, Object>>> columnListMap = new HashMap<>(16);
        sql = String.format(SQL_QUERY_COLUMN_TOTAL, quote(currentDb.get()));
        if(StringUtils.isNotBlank(allTable)){
            sql += String.format(SQL_QUERY_COLUMN, allTable);
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

    protected Map<String, String> queryPrimaryKeyMap() throws SQLException {
        Map<String, String> primaryKeyMap = new HashMap<>(16);
        sql = String.format(SQL_QUERY_PRIMARY_KEY_TOTAL, quote(currentDb.get()));
        if (StringUtils.isNotBlank(allTable)){
            sql += String.format(SQL_QUERY_PRIMARY_KEY, allTable);
        }
        try (ResultSet rs = statement.get().executeQuery(sql)){
            while (rs.next()){
                primaryKeyMap.put(rs.getString(1), rs.getString(2));
            }
        }
        return primaryKeyMap;
    }

    protected Map<String, String> queryCreatedTimeMap() throws SQLException {
        Map<String, String> primaryKeyMap = new HashMap<>(16);
        sql = String.format(SQL_QUERY_TABLE_CREATE_TIME_TOTAL, quote(currentDb.get()));
        if (StringUtils.isNotBlank(allTable)){
            sql += String.format(SQL_QUERY_TABLE_CREATE_TIME, allTable);
        }
        try (ResultSet rs = statement.get().executeQuery(sql)){
            while (rs.next()){
                primaryKeyMap.put(rs.getString(1), rs.getString(2));
            }
        }
        return primaryKeyMap;
    }

    /**
     * 小于20张表时采用in语法
     * @throws SQLException 执行sql出现的异常
     */
    @Override
    protected void init() throws SQLException {
        if(tableList.size() <= MAX_TABLE_SIZE){
            StringBuilder stringBuilder = new StringBuilder(2 * tableList.size());
            for(int index=0;index<tableList.size();index++){
                stringBuilder.append(quote((String) tableList.get(index)));
                if(index!=tableList.size()-1){
                    stringBuilder.append(ConstantValue.COMMA_SYMBOL);
                }
            }
            allTable = stringBuilder.toString();
        }
        tablePropertiesMap = queryTableProperties();
        LOG.info("tablePropertiesMap = {}", tablePropertiesMap);
        columnListMap = queryColumnList();
        LOG.info("columnListMap = {}", columnListMap);
        indexListMap = queryIndexList();
        LOG.info("indexListMap = {}", indexListMap);
        primaryKeyMap = queryPrimaryKeyMap();
        LOG.info("primaryKeyMap = {}", primaryKeyMap);
        createdTimeMap = queryCreatedTimeMap();
        LOG.info("createdTimeMap = {}", createdTimeMap);
    }
}
