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

package com.dtstack.flinkx.metadatavertica.inputformat;

import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_DATA_TYPE;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_DEFAULT;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_INDEX;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_NAME;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_NULL;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_COMMENT;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_CREATE_TIME;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadata.MetaDataCons.RESULT_SET_COLUMN_DEF;
import static com.dtstack.flinkx.metadata.MetaDataCons.RESULT_SET_COLUMN_NAME;
import static com.dtstack.flinkx.metadata.MetaDataCons.RESULT_SET_IS_NULLABLE;
import static com.dtstack.flinkx.metadata.MetaDataCons.RESULT_SET_ORDINAL_POSITION;
import static com.dtstack.flinkx.metadata.MetaDataCons.RESULT_SET_TABLE_NAME;
import static com.dtstack.flinkx.metadata.MetaDataCons.RESULT_SET_TYPE_NAME;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.SQL_COMMENT;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.SQL_CREATE_TIME;

/** 读取vertica的元数据
 * @author kunni@dtstack.com
 */
public class MetadataverticaInputFormat extends BaseMetadataInputFormat {

    protected Map<String, String> createTimeMap;

    protected Map<String, String> commentMap;

    @Override
    protected List<Object> showTables() throws SQLException {
        List<Object> table = new LinkedList<>();
        ResultSet resultSet = connection.get().getMetaData().getTables(null, currentDb.get(), null, null);
        while (resultSet.next()){
            table.add(resultSet.getString(RESULT_SET_TABLE_NAME));
        }
        return table;
    }

    @Override
    protected void switchDatabase(String databaseName) {

    }

    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);
        result.put(KEY_TABLE_PROPERTIES, queryTableProp(tableName));
        result.put(KEY_COLUMN, queryColumn(tableName));
        return result;
    }

    @Override
    protected String quote(String name) {
        return null;
    }

    @Override
    protected void init() throws SQLException {
        queryCreateTime();
        queryComment();
    }

    public List<Map<String, Object>> queryColumn(String tableName) throws SQLException {
        List<Map<String, Object>> columns = new LinkedList<>();
        ResultSet resultSet = connection.get().getMetaData().getColumns(null, currentDb.get(), tableName, null);
        while (resultSet.next()){
            Map<String, Object> map = new HashMap<>(16);
            map.put(KEY_COLUMN_NAME, resultSet.getString(RESULT_SET_COLUMN_NAME));
            map.put(KEY_COLUMN_DATA_TYPE, resultSet.getString(RESULT_SET_TYPE_NAME));
            map.put(KEY_COLUMN_INDEX, resultSet.getString(RESULT_SET_ORDINAL_POSITION));
            map.put(KEY_COLUMN_NULL, resultSet.getString(RESULT_SET_IS_NULLABLE));
            map.put(KEY_COLUMN_DEFAULT, resultSet.getString(RESULT_SET_COLUMN_DEF));
            columns.add(map);
        }
        return columns;
    }


    public Map<String, String> queryTableProp(String tableName){
        Map<String, String> tableProperties = new HashMap<>(16);
        tableProperties.put(KEY_TABLE_CREATE_TIME,  createTimeMap.get(tableName));
        tableProperties.put(KEY_TABLE_COMMENT, commentMap.get(tableName));
        return tableProperties;
    }

    public void queryCreateTime() throws SQLException {
        createTimeMap = new HashMap<>(16);
        String sql = String.format(SQL_CREATE_TIME, currentDb.get());
        try(ResultSet resultSet = executeQuery0(sql, statement.get())){
            while (resultSet.next()){
                createTimeMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        }
    }

    public void queryComment() throws SQLException {
        commentMap = new HashMap<>(16);
        String sql = String.format(SQL_COMMENT, currentDb.get());
        try(ResultSet resultSet = executeQuery0(sql, statement.get())){
            while (resultSet.next()){
                commentMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        }
    }
}
