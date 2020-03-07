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
import com.fasterxml.jackson.databind.ObjectMapper;
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
    protected List<String> column = new ArrayList<>();
    protected List<String> partitionColumn = new ArrayList<>();
    protected String dbUrl;
    protected String username;
    protected String password;
    protected List<String> table;

    protected int numPartitions;

    protected ResultSet resultSet;
    protected int columnCount;

    protected boolean hasNext = true;

    protected Connection connection;
    protected Statement statement;
    protected int count;

    protected Map<String, Map<String, String>> filterData;


    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        try {
            LOG.info("inputSplit = {}", inputSplit);
            hasNext = true;
            count = table.size();
            resultSet = excuteSql(buildDescSql(((MetadataInputSplit) inputSplit).getTable(), true));
            columnCount = resultSet.getMetaData().getColumnCount();
            filterData = transformDataToMap(resultSet);
            getTableColumns(((MetadataInputSplit) inputSplit).getTable());
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("openInternal 异常，具体信息为：", e);
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        minNumSplits = table.size();
        InputSplit[] inputSplits = new MetadataInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new MetadataInputSplit(i, numPartitions, dbUrl, table.get(i));
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        row = new Row(1);
        row.setField(0, objectMapper.writeValueAsString(selectUsedData(filterData)));

        hasNext = false;

        column.clear();
        partitionColumn.clear();

        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
        DBUtil.closeDBResources(resultSet, statement, connection, true);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    public ResultSet excuteSql(String sql) throws ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        ResultSet result = null;
        try {
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.createStatement();
            result = statement.executeQuery(sql);
        } catch (SQLException e) {
            throw new RuntimeException("查询异常，请检查相关配置:"
                    + dbUrl + " " + username + " " + password + "当前查询语句为: " + sql);
        }
        return result;
    }

    public String buildDescSql(String table, boolean formatted) {
        String sql = "";
        if (formatted) {
            sql = "DESC FORMATTED " + table;
        } else {
            sql = "DESC " + table;
        }
        return sql;
    }

    /**
     * 从查询结果中构建Map
     *
     * @param resultSet
     * @return
     */
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
            throw new RuntimeException("查询结果转化异常", e);
        }
        return result;
    }

    /**
     * 从查询的结果中构建需要的信息
     *
     * @param map
     * @return
     */
    public Map<String, Object> selectUsedData(Map<String, Map<String, String>> map) {
        Map<String, Object> result = new HashMap<>();
        Map<String, String> temp = new HashMap<>();
        List<Map> tempColumnList = new ArrayList<>();
        List<Map> tempPartitionColumnList = new ArrayList<>();

        for (String item : column) {
            tempColumnList.add(setColumnMap(item, map.get(item), column.indexOf(item)));
        }
        result.put("column", tempColumnList);

        for (String item : partitionColumn) {
            tempPartitionColumnList.add(setColumnMap(item, map.get(item), partitionColumn.indexOf(item)));
        }
        result.put("partitionColumn", tempPartitionColumnList);

        // TODO 根据不同的inputformat确定不同的文件存储方式
        String storedType = "text";
        if (map.get("InputFormat").keySet().toString().contains("TextInputformat")) {
            storedType = "text";
        }
        if (map.get("InputFormat").keySet().toString().contains("MapredParquetInputFormat")) {
            storedType = "Parquet";
        }

        result.put("table", table.get(1));

        temp.put("comment", map.get("Table Parameters").get("comment"));
        temp.put("storedType", storedType);
        result.put("tablePropertites", temp);

        // TODO 下面的这些还不确定如何获取
        result.put("operateType", "createTable");
        return result;
    }

    /**
     * 获取查询表中字段名列表
     *
     * @param table
     * @return
     */
    public void getTableColumns(String table) {
        boolean flag = false;
        try {
            ResultSet temp = excuteSql(buildDescSql(table, false));
            while (temp.next()) {
                if (temp.getString(1).trim().contains("Partition Information")) {
                    flag = true;
                }
                if (flag) {
                    partitionColumn.add(temp.getString(1).trim());
                } else {
                    column.add(temp.getString(1));
                }
            }
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("获取字段名列表异常");
        }
        // 除去多余的字段
        partitionColumn.remove("# Partition Information");
        partitionColumn.remove("");
        partitionColumn.remove("# col_name");
        column.remove("");
    }

    /**
     * 通过查询得到的结果构建字段名相应的信息
     *
     * @param columnName
     * @param map
     * @param index
     * @return
     */
    public Map<String, Object> setColumnMap(String columnName, Map<String, String> map, int index) {
        Map<String, Object> result = new HashMap<>();
        result.put("name", columnName);
        if (map.keySet().toArray()[0] == null) {
            result.put("type", map.keySet().toArray()[1]);
        } else {
            result.put("type", map.keySet().toArray()[0]);
        }
        result.put("index", index);
        result.put("comment", map.values().toArray()[0]);
        return result;
    }
}
