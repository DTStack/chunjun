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

package com.dtstack.flinkx.metadatasqlserver.inputformat;

import com.dtstack.flinkx.metadata.MetaDataCons;
import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import com.dtstack.flinkx.metadatasqlserver.constants.SqlserverMetadataCons;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.flink.types.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author : kunni@dtstack.com
 * @date : 2020/08/06
 */

public class MetadatasqlserverInputFormat extends BaseMetadataInputFormat {

    @Override
    protected List<String> showTables() throws SQLException {
        List<String> tableNameList = new LinkedList<>();
        try (ResultSet rs = statement.get().executeQuery(SqlserverMetadataCons.SQL_SHOW_TABLES)) {
            while (rs.next()) {
                tableNameList.add(rs.getString(1));
            }
        }
        return tableNameList;
    }

    @Override
    protected void switchDatabase(String databaseName) throws SQLException {
        statement.get().execute(String.format(SqlserverMetadataCons.SQL_SWITCH_DATABASE, quote(databaseName)));
    }

    @Override
    protected Row nextRecordInternal(Row row) {
        Map<String, Object> metaData = new HashMap<>(16);
        metaData.put(MetaDataCons.KEY_OPERA_TYPE, MetaDataCons.DEFAULT_OPERA_TYPE);

        String tableName = tableIterator.next();
        metaData.put(SqlserverMetadataCons.KEY_DATABASE, currentDb.get());
        metaData.put(MetaDataCons.KEY_SCHEMA, tableName);
        metaData.put(MetaDataCons.KEY_TABLE, tableName);
        metaData.put(MetaDataCons.KEY_TOTAL_TABLE, totalTable);
        metaData.put(MetaDataCons.KEY_RESOLVED_TABLE, resolvedTable.incrementAndGet());

        try {
            metaData.putAll(queryMetaData(tableName));
            metaData.put(MetaDataCons.KEY_QUERY_SUCCESS, true);
        } catch (Exception e) {
            metaData.put(MetaDataCons.KEY_QUERY_SUCCESS, false);
            metaData.put(MetaDataCons.KEY_ERROR_MSG, ExceptionUtil.getErrorMessage(e));
        }

        return Row.of(metaData);
    }

    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {
        Map<String, Object> result = new HashMap<>(16);
        Map<String, Object> tableProperties = queryTableProp(tableName);

        return result;
    }

    protected Map<String, Object> queryTableProp(String tableName){
        return null;
    }

    @Override
    protected String quote(String name) {
        return "'" + name + "'";
    }
}
