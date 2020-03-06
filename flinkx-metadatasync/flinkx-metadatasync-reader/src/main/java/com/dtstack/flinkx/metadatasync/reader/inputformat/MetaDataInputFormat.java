/**
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

package com.dtstack.flinkx.metadatasync.reader.inputformat;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/5
 * @description :
 */
public class MetaDataInputFormat extends RichInputFormat {
    protected List<String> metaColumns;
    protected String dbUrl;
    protected String username;
    protected String password;
    protected String table;

    protected ResultSet resultSet;
    protected int columnCount;

    protected boolean hasNext = true;

    protected Connection connection;
    protected Statement statement;

    protected Map<String, Map<String, String>> filterData;

    protected String jsonRow;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        try {
            resultSet = excuteSql(buildDescSql(table, true));
            columnCount = resultSet.getMetaData().getColumnCount();
            filterData = transformDataToMap(resultSet);
            jsonRow = mapToJson(filterData);
            metaColumns = getTableColumns(table);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        InputSplit[] inputSplits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        row = new Row(1);
        row.setField(0, objectMapper.writeValueAsString(selectUsedData(filterData))
                .replace("\\", ""));
        hasNext = false;
        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
        DBUtil.closeDBResources(resultSet, statement, connection, true);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (hasNext) {
            return false;
        }
        return true;
    }

    public ResultSet excuteSql(String sql) throws ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        ResultSet result = null;
        try {
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.createStatement();
            result = statement.executeQuery(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public String buildDescSql(String table, boolean formatted) {
        String sql = "";
        if (formatted) {
            sql = String.format("DESC FORMATTED " + table);
        } else {
            sql = String.format("DESC " + table);
        }
        return sql;
    }

    public static Map<String, Map<String, String>> transformDataToMap(ResultSet resultSet) {
        Map<String, Map<String, String>> result = new HashMap<>();
        String tempK = "";
        Map<String, String> map = new HashMap<>();
        try {
            while (resultSet.next()) {
                String tempK1 = resultSet.getString(1).replace(":", "").trim();
                String tempK2 = resultSet.getString(2);
                String tempVal = resultSet.getString(3);
                if (!tempK1.isEmpty()) {
                    if (!map.isEmpty()) {
                        result.put(tempK, map);
                        map = new HashMap<>();
                    }
                    tempK = tempK1;
                    map.put(tempK2, tempVal);
                } else {
                    if (tempK2 != null) {
                        map.put(tempK2.trim(), tempVal);
                    } else {
                        map.put(tempK2, tempVal);
                    }
                    continue;
                }
                result.put(tempK, map);
            }
            result.put(tempK, map);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public String mapToJson(Map<String, Map<String, String>> map) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String result = "";
        Integer count = map.size();
        Object[] temp = map.keySet().toArray();
        for (int i = 0; i < count; i++) {
            map.get(temp[i]).remove(null);
        }
        result = objectMapper.writeValueAsString(map);
        return result;
    }

    public Map<String, Object> selectUsedData(Map<String, Map<String, String>> map) {
        Map<String, Object> result = new HashMap<>();
        Map<String, String> temp = new HashMap<>();
        List<Map> tempList = new ArrayList<>();

        // TODO 根据查询得到的字段名组合需要的信息
        for (String item : metaColumns) {
            tempList.add(setColumnMap(item, map.get(item), metaColumns.indexOf(item)));
        }

        // TODO 根据不同的inputformat确定不同的文件存储方式
        String storedType = "text";
        if (map.get("InputFormat").keySet().toString().contains("TextInputformat")) {
            storedType = "text";
        }

        result.put("column", tempList);
        result.put("table", table);
        temp.put("comment", map.get("Table Parameters").get("comment"));
        temp.put("storedType", storedType);
        result.put("tablePropertites", temp);
        // TODO 下面的这些还不确定如何获取
        result.put("operateType", "createTable");
        return result;
    }

    public List<String> getTableColumns(String table) {
        List<String> result = new ArrayList<>();
        try {
            ResultSet temp = excuteSql(buildDescSql(table, false));
            while (temp.next()) {
                result.add((String) temp.getObject(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public Map<String, Object> setColumnMap(String columnName, Map<String, String> map, int index) {
        Map<String, Object> result = new HashMap<>();
        result.put("name", columnName);
        result.put("type", map.keySet().toArray()[0]);
        result.put("index", index);
        result.put("comment", map.values().toArray()[0]);
        return result;
    }
}
