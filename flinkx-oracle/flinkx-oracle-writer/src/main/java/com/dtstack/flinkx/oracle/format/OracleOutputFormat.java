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
package com.dtstack.flinkx.oracle.format;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.flink.types.Row;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2019/09/20
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class OracleOutputFormat extends JdbcOutputFormat {

    @Override
    protected Object getField(Row row, int index) {
        Object field = super.getField(row, index);
        String type = columnType.get(index);

        //oracle timestamp to oracle varchar or varchar2 or long field format
        if (!(field instanceof Timestamp)){
            return field;
        }

        if (type.equalsIgnoreCase(ColumnType.VARCHAR.name()) || type.equalsIgnoreCase(ColumnType.VARCHAR2.name())){
            SimpleDateFormat format = DateUtil.getDateTimeFormatter();
            field= format.format(field);
        }

        if (type.equalsIgnoreCase(ColumnType.LONG.name()) ){
            field = ((Timestamp) field).getTime();
        }
        return field;
    }

    @Override
    protected List<String> probeFullColumns(String table, Connection dbConn) throws SQLException {
        String schema =null;

        String[] parts = table.split("\\.");
        if(parts.length == 2) {
            schema = parts[0].toUpperCase();
            table = parts[1];
        }

        List<String> ret = new ArrayList<>();
        ResultSet rs = dbConn.getMetaData().getColumns(null, schema, table, null);
        while(rs.next()) {
            ret.add(rs.getString("COLUMN_NAME"));
        }
        return ret;
    }

    @Override
    protected Map<String, List<String>> probePrimaryKeys(String table, Connection dbConn) throws SQLException {
        Map<String, List<String>> map = new HashMap<>(16);
        PreparedStatement ps = dbConn.prepareStatement(String.format(GET_INDEX_SQL,table));
        ResultSet rs = ps.executeQuery();

        while(rs.next()) {
            String indexName = rs.getString("INDEX_NAME");
            if(!map.containsKey(indexName)) {
                map.put(indexName,new ArrayList<>());
            }
            map.get(indexName).add(rs.getString("COLUMN_NAME"));
        }
        Map<String,List<String>> retMap = new HashMap<>((map.size()<<2)/3);
        for(Map.Entry<String,List<String>> entry: map.entrySet()) {
            String k = entry.getKey();
            List<String> v = entry.getValue();
            if(v!=null && v.size() != 0 && v.get(0) != null) {
                retMap.put(k, v);
            }
        }
        return retMap;
    }
}
