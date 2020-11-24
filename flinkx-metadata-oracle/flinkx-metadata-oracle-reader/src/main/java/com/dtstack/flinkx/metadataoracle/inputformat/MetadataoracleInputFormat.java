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
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_PARTITION_COLUMNS;
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
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_CREATE_TIME;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_MAX_NUMBER;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_NUMBER;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_PARTITION_KEY;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_PRIMARY_KEY;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.KEY_TABLE_TYPE;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.NUMBER_PRECISION;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_PARTITION_KEY;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_COLUMN;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_COLUMN_TOTAL;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_INDEX;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_INDEX_TOTAL;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_PRIMARY_KEY;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_PRIMARY_KEY_TOTAL;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_TABLE_CREATE_TIME;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_TABLE_CREATE_TIME_TOTAL;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_TABLE_PARTITION_KEY;
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
    private Map<String, List<Map<String, String>>> columnListMap;

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

    /**
     * 分区列
     */
    private Map<String, String> partitionMap;

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

    /**
     * 从预先查询好的map中取出信息
     * @param tableName 表名
     * @return 表的元数据信息
     * @throws SQLException 异常
     */
    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);
        // 如果当前map中没有，说明要重新取值
        if(!tablePropertiesMap.containsKey(tableName)){
            init();
        }
        Map<String, String> tableProperties = tablePropertiesMap.get(tableName);
        tableProperties.put(KEY_TABLE_CREATE_TIME, createdTimeMap.get(tableName));
        List<Map<String, String>> columnList = columnListMap.get(tableName);
        List<Map<String, String>> indexList = indexListMap.get(tableName);
        String primaryColumn = primaryKeyMap.get(tableName);
        String partitionKey = partitionMap.get(tableName);
        List<Map<String, String>> partitionColumnList = new ArrayList<>();
        for(Map<String, String> map : columnList){
            if(StringUtils.equals(map.get(KEY_COLUMN_NAME), primaryColumn)){
                map.put(KEY_COLUMN_PRIMARY, KEY_TRUE);
            }else{
                map.put(KEY_COLUMN_PRIMARY, KEY_FALSE);
            }
            if(StringUtils.equals(map.get(KEY_COLUMN_NAME), partitionKey)){
                partitionColumnList.add(map);
            }
        }
        result.put(KEY_PARTITION_COLUMNS, partitionColumnList);
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
        LOG.info("querySQL: {}", sql);
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
        LOG.info("querySQL: {}", sql);
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

    protected Map<String, List<Map<String, String>>> queryColumnList() throws SQLException {
        Map<String, List<Map<String, String>>> columnListMap = new HashMap<>(16);
        sql = String.format(SQL_QUERY_COLUMN_TOTAL, quote(currentDb.get()));
        if(StringUtils.isNotBlank(allTable)){
            sql += String.format(SQL_QUERY_COLUMN, allTable);
        }
        LOG.info("querySQL: {}", sql);
        try (ResultSet rs = statement.get().executeQuery(sql)) {
            while (rs.next()) {
                Map<String, String> column = new HashMap<>(16);
                // oracle中，resultSet的LONG、LONG ROW类型要放第一个取出来
                column.put(KEY_COLUMN_DEFAULT, rs.getString(5));
                column.put(KEY_COLUMN_NAME, rs.getString(1));
                String type = rs.getString(2);
                String length = rs.getString(7);
                if(StringUtils.equals(type, KEY_NUMBER)){
                    String precision = rs.getString(9);
                    String scale = rs.getString(10);
                    column.put(KEY_COLUMN_TYPE, String.format(NUMBER_PRECISION, precision==null?length:precision, scale==null?KEY_MAX_NUMBER:scale));
                }else {
                    column.put(KEY_COLUMN_TYPE, type);
                }
                column.put(KEY_COLUMN_COMMENT, rs.getString(3));
                String tableName = rs.getString(4);
                column.put(KEY_COLUMN_NULL, rs.getString(6));
                column.put(KEY_COLUMN_SCALE, length);
                String index = rs.getString(8);
                if(columnListMap.containsKey(tableName)){
                    column.put(KEY_COLUMN_INDEX, index);
                    columnListMap.get(tableName).add(column);
                }else {
                    List<Map<String, String>>columnList  = new LinkedList<>();
                    column.put(KEY_COLUMN_INDEX, index);
                    columnList.add(column);
                    columnListMap.put(tableName, columnList);
                }
            }
        }
        return columnListMap;
    }

    /**
     * 表名和某个特定属性映射的map
     * @return 映射map
     * @throws SQLException sql异常
     */
    protected Map<String, String> queryTableKeyMap(String type) throws SQLException {
        Map<String, String> primaryKeyMap = new HashMap<>(16);
        switch (type){
            case KEY_PRIMARY_KEY:{
                sql = String.format(SQL_QUERY_PRIMARY_KEY_TOTAL, quote(currentDb.get()));
                if (StringUtils.isNotBlank(allTable)){
                    sql += String.format(SQL_QUERY_PRIMARY_KEY, allTable);
                }
                break;
            }
            case KEY_CREATE_TIME:{
                sql = String.format(SQL_QUERY_TABLE_CREATE_TIME_TOTAL, quote(currentDb.get()));
                if (StringUtils.isNotBlank(allTable)){
                    sql += String.format(SQL_QUERY_TABLE_CREATE_TIME, allTable);
                }
                break;
            }
            case KEY_PARTITION_KEY:{
                sql = String.format(SQL_PARTITION_KEY, quote(currentDb.get()));
                if (StringUtils.isNotBlank(allTable)){
                    sql += String.format(SQL_QUERY_TABLE_PARTITION_KEY, allTable);
                }
                break;
            }
            default: break;
        }
        LOG.info("querySQL: {}", sql);
        try (ResultSet rs = statement.get().executeQuery(sql)){
            while (rs.next()){
                primaryKeyMap.put(rs.getString(1), rs.getString(2));
            }
        }
        return primaryKeyMap;
    }

    /**
     * 每隔20张表进行一次查询
     * @throws SQLException 执行sql出现的异常
     */
    @Override
    protected void init() throws SQLException {
        // 没有表则退出
        if(tableList.size()==0){
            return;
        }
        allTable = null;
        if (start < tableList.size()){
            // 取出子数组，注意避免越界
            List<Object> splitTableList = tableList.subList(start, Math.min(start+MAX_TABLE_SIZE, tableList.size()));
            StringBuilder stringBuilder = new StringBuilder(2 * splitTableList.size());
            for(int index=0;index<splitTableList.size();index++){
                stringBuilder.append(quote((String) splitTableList.get(index)));
                if(index!=splitTableList.size()-1){
                    stringBuilder.append(ConstantValue.COMMA_SYMBOL);
                }
            }
            allTable = stringBuilder.toString();
            tablePropertiesMap = queryTableProperties();
            columnListMap = queryColumnList();
            indexListMap = queryIndexList();
            primaryKeyMap =  queryTableKeyMap(KEY_PRIMARY_KEY);
            createdTimeMap = queryTableKeyMap(KEY_CREATE_TIME);
            partitionMap = queryTableKeyMap(KEY_PARTITION_KEY);
        }
    }
}
