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

import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_INDEX;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_QUERY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_SHOW_DATABASES;
import static com.dtstack.flinkx.metadataoracle.constants.OracleMetaDataCons.SQL_SHOW_TABLES;

/**
 * @author : kunni@dtstack.com
 * @date : 2020/6/9
 * @description :Oracle元数据可在系统表中查询
 */

public class MetadataoracleInputFormat extends BaseMetadataInputFormat {

    @Override
    protected List<String> showTables() throws SQLException {
        List<String> tableNameList = new LinkedList<>();
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
        Map<String, String> tableProperties = queryTableProperties(tableName);
        List<Map<String, Object>> columnList = queryColumnList(tableName);
        List<Map<String, String>> indexList = queryIndexList(tableName);
        result.put(KEY_TABLE_PROPERTIES, tableProperties);
        result.put(KEY_COLUMN, columnList);
        result.put(KEY_COLUMN_INDEX, indexList);
        return result;
    }

    Map<String, String> queryTableProperties(String tableName) throws SQLException {
        Map<String, String> tableProperties = new HashMap<>(16);
        String sql = String.format(SQL_QUERY_TABLE_PROPERTIES, quote(currentDb.get()), quote(tableName));
        try (ResultSet rs = statement.get().executeQuery(sql)) {
            while (rs.next()) {
                tableProperties.put(KEY_TOTAL_SIZE, rs.getString(1));
                tableProperties.put(KEY_COLUMN_COMMENT, rs.getString(2));
                tableProperties.put(KEY_TABLE_TYPE, rs.getString(3));
                tableProperties.put(KEY_CREATE_TIME, rs.getString(4));
                tableProperties.put(KEY_ROWS, rs.getString(5));
            }
        }
        return tableProperties;
    }

    List<Map<String, String>> queryIndexList(String tableName) throws SQLException {
        List<Map<String, String>>indexList  = new LinkedList<>();
        String sql = String.format(SQL_QUERY_INDEX, quote(currentDb.get()), quote(tableName));
        try (ResultSet rs = statement.get().executeQuery(sql)) {
            while (rs.next()) {
                Map<String, String> column = new HashMap<>(16);
                column.put(KEY_COLUMN_NAME, rs.getString(1));
                column.put(KEY_INDEX_COLUMN_NAME, rs.getString(2));
                column.put(KEY_COLUMN_TYPE, rs.getString(3));
                indexList.add(column);
            }
        }
        return indexList;
    }

    List<Map<String, Object>> queryColumnList(String tableName) throws SQLException {
        List<Map<String, Object>>columnList  = new LinkedList<>();
        String sql = String.format(SQL_QUERY_COLUMN, quote(currentDb.get()), quote(tableName));
        try (ResultSet rs = statement.get().executeQuery(sql)) {
            while (rs.next()) {
                Map<String, Object> column = new HashMap<>(16);
                column.put(KEY_COLUMN_NAME, rs.getString(1));
                column.put(KEY_COLUMN_TYPE, rs.getString(2));
                column.put(KEY_COLUMN_COMMENT, rs.getString(3));
                column.put(KEY_COLUMN_INDEX, CollectionUtils.size(columnList)+1);
                columnList.add(column);
            }
        }
        return columnList;
    }

}
