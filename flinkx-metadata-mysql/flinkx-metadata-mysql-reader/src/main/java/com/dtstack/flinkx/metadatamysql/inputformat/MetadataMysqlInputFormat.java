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

package com.dtstack.flinkx.metadatamysql.inputformat;

import com.dtstack.flinkx.metadatatidb.inputformat.MetadatatidbInputFormat;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.*;
/**
 * @author : kunni@dtstack.com
 * @date : 2020/6/8
 */

public class MetadataMysqlInputFormat extends MetadatatidbInputFormat {

    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);
        Map<String, String> tableProp = queryTableProp(tableName);
        List<Map<String, Object>> column = queryColumn(tableName);
        List<Map<String, String>> index = queryIndex(tableName);
        result.put(KEY_TABLE_PROPERTIES, tableProp);
        result.put(KEY_COLUMN, column);
        result.put(KEY_COLUMN_INDEX, index);
        return result;
    }

    /**
     * @description add engine、table_type、row_format
     */
    @Override
    public Map<String, String> queryTableProp(String tableName) throws SQLException {
        Map<String, String> tableProp = new HashMap<>(16);
        String sql = String.format(SQL_QUERY_TABLE_INFO, quote(currentDb.get()), quote(tableName));
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                tableProp.put(KEY_TABLE_TYPE, RESULT_TABLE_TYPE);
                tableProp.put(KEY_ENGINE, rs.getString(RESULT_ENGINE));
                tableProp.put(KEY_ROW_FORMAT, rs.getString(RESULT_ROW_FORMAT));
                tableProp.put(KEY_ROWS, rs.getString(RESULT_ROWS));
                tableProp.put(KEY_TOTAL_SIZE, rs.getString(RESULT_DATA_LENGTH));
                tableProp.put(KEY_CREATE_TIME, rs.getString(RESULT_CREATE_TIME));
                tableProp.put(KEY_COLUMN_COMMENT, rs.getString(RESULT_TABLE_COMMENT));
            }
        } catch (SQLException e) {
            throw new SQLException(ExceptionUtils.getMessage(e));
        }
        return tableProp;
    }

    protected List<Map<String, Object>> queryColumn(String tableName) throws SQLException {
        List<Map<String, Object>> column = new LinkedList<>();
        String sql = String.format(SQL_QUERY_COLUMN, tableName);
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            int pos = 1;
            while (rs.next()) {
                Map<String, Object> perColumn = new HashMap<>(16);
                perColumn.put(KEY_COLUMN_NAME, rs.getString(RESULT_FIELD));
                perColumn.put(KEY_COLUMN_TYPE, rs.getString(KEY_COLUMN_TYPE));
                perColumn.put(KEY_COLUMN_COMMENT, rs.getString(RESULT_COMMENT));
                perColumn.put(KEY_COLUMN_INDEX, pos++);
                column.add(perColumn);
            }
        } catch (SQLException e) {
            throw new SQLException(ExceptionUtils.getMessage(e));
        }
        return column;
    }

    protected List<Map<String, String>> queryIndex(String tableName) throws SQLException {
        List<Map<String, String>> index = new LinkedList<>();
        String sql = String.format(SQL_QUERY_INDEX, tableName);
        try(Statement st = connection.get().createStatement();
            ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                Map<String, String> perIndex = new HashMap<>(16);
                perIndex.put(KEY_COLUMN_NAME, rs.getString(RESULT_KEY_NAME));
                perIndex.put(KEY_KEY_NAME, rs.getString(RESULT_COLUMN_NAME));
                perIndex.put(KEY_COLUMN_TYPE, rs.getString(RESULT_INDEX_TYPE));
                perIndex.put(KEY_COLUMN_COMMENT, rs.getString(RESULT_INDEX_COMMENT));
                index.add(perIndex);
            }
        } catch (SQLException e) {
            throw new SQLException(ExceptionUtils.getMessage(e));
        }
        return index;
    }
}
