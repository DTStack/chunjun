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

package com.dtstack.flinkx.metadataphoenix.inputformat;

import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_DATA_TYPE;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_INDEX;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_COLUMN_NAME;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadataphoenix.constants.PhoenixMetadataCons.SQL_TABLE_PROPERTIES;

/**
 * @author kunni@Dtstack.com
 */

public class MetadataphoenixInputFormat extends BaseMetadataInputFormat {
    @Override
    protected List<Object> showTables() throws SQLException{
        List<Object> table = new LinkedList<>();
        String sql = String.format(SQL_TABLE_PROPERTIES, currentDb.get());
        ResultSet resultSet = executeQuery0(sql, statement.get());
        while (resultSet.next()){
            table.add(resultSet.getString(1));
        }
        return table;
    }

    @Override
    protected void switchDatabase(String databaseName) {
    }

    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);
        Map<String, String> tableProp = queryTableProp(tableName);
        List<Map<String, Object>> column = queryColumn(tableName);
        result.put(KEY_TABLE_PROPERTIES, tableProp);
        result.put(KEY_COLUMN, column);
        return result;
    }

    @Override
    protected String quote(String name) {
        return null;
    }

    public Map<String, String> queryTableProp(String tableName){
        Map<String, String> tableProp = new HashMap<>();
        return tableProp;
    }

    public List<Map<String, Object>> queryColumn(String tableName) throws SQLException {
        List<Map<String, Object>> column = new LinkedList<>();
        ResultSet resultSet = connection.get().getMetaData().getColumns(null, currentDb.get(), tableName, null);
        while (resultSet.next()){
            Map<String, Object> map = new HashMap<>();
            map.put(KEY_COLUMN_NAME, resultSet.getString("COLUMN_NAME"));
            map.put(KEY_COLUMN_DATA_TYPE, resultSet.getString("TYPE_NAME"));
            map.put(KEY_COLUMN_INDEX, resultSet.getString("ORDINAL_POSITION"));
            column.add(map);
        }
        return column;
    }

}
