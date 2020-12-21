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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import com.dtstack.flinkx.metadataphoenix.util.ZkHelper;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang.StringUtils;

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
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_CREATE_TIME;
import static com.dtstack.flinkx.metadata.MetaDataCons.KEY_TABLE_PROPERTIES;
import static com.dtstack.flinkx.metadata.MetaDataCons.RESULT_SET_COLUMN_NAME;
import static com.dtstack.flinkx.metadata.MetaDataCons.RESULT_SET_ORDINAL_POSITION;
import static com.dtstack.flinkx.metadata.MetaDataCons.RESULT_SET_TABLE_NAME;
import static com.dtstack.flinkx.metadata.MetaDataCons.RESULT_SET_TYPE_NAME;
import static com.dtstack.flinkx.metadataphoenix.util.ZkHelper.DEFAULT_PATH;

/**
 * @author kunni@Dtstack.com
 */

public class MetadataphoenixInputFormat extends BaseMetadataInputFormat {

    private Map<String, Long> createTimeMap;

    @Override
    protected List<Object> showTables() throws SQLException{
        String schema = currentDb.get();
        if(StringUtils.isBlank(schema)){
            schema = null;
        }
        List<Object> table = new LinkedList<>();
        ResultSet resultSet = connection.get().getMetaData().getTables(null, schema, null, null);
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
        Map<String, Object> tableProp = queryTableProp(tableName);
        List<Map<String, Object>> column = queryColumn(tableName);
        result.put(KEY_TABLE_PROPERTIES, tableProp);
        result.put(KEY_COLUMN, column);
        return result;
    }

    @Override
    protected String quote(String name) {
        return name;
    }

    public Map<String, Object> queryTableProp(String tableName){
        Map<String, Object> tableProp = new HashMap<>();
        tableProp.put(KEY_TABLE_CREATE_TIME, createTimeMap.get(tableName));
        return tableProp;
    }

    public List<Map<String, Object>> queryColumn(String tableName) throws SQLException {
        List<Map<String, Object>> column = new LinkedList<>();
        ResultSet resultSet = connection.get().getMetaData().getColumns(null, currentDb.get(), tableName, null);
        while (resultSet.next()){
            Map<String, Object> map = new HashMap<>();
            map.put(KEY_COLUMN_NAME, resultSet.getString(RESULT_SET_COLUMN_NAME));
            map.put(KEY_COLUMN_DATA_TYPE, resultSet.getString(RESULT_SET_TYPE_NAME));
            map.put(KEY_COLUMN_INDEX, resultSet.getString(RESULT_SET_ORDINAL_POSITION));
            column.add(map);
        }
        return column;
    }


    protected Map<String, Long> queryCreateTimeMap(String hosts) {
        Map<String, Long> createTimeMap = new HashMap<>(16);
        try{
            ZkHelper.createSingleZkClient(hosts, ZkHelper.DEFAULT_TIMEOUT);
            List<String> tables = ZkHelper.getChildren(DEFAULT_PATH);
            if(tables != null){
                for(String table : tables){
                    createTimeMap.put(table, ZkHelper.getStat(DEFAULT_PATH + ConstantValue.SINGLE_SLASH_SYMBOL + table));
                }
            }
            ZkHelper.closeZooKeeper();
        }catch (Exception e){
            LOG.error("query createTime map failed, error {}", ExceptionUtil.getErrorMessage(e));
        }
        return createTimeMap;
    }


    @Override
    protected void init() {
        String hosts = dbUrl.substring("jdbc:phoenix:".length());
        createTimeMap = queryCreateTimeMap(hosts);
    }
}
