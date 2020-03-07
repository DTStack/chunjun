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
    protected int numPartitions;

    protected List<String> tableColumn = new ArrayList<>();
    protected List<String> partitionColumn = new ArrayList<>();
    protected int columnCount;

    protected String dbUrl;
    protected String username;
    protected String password;
    protected List<String> table;

    protected Connection connection;
    protected Statement statement;

    protected ResultSet resultSet;
    protected String queryTable;

    protected boolean hasNext = true;

    protected Map<String, Map<String, String>> filterData;

    protected Map<String, String> errorMessage;
    protected Map<String, Object> currentMessage;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.createStatement();
        } catch (SQLException | ClassNotFoundException e) {
            errorMessage = setErrorMessage(e, "can not connect to hive! current dbUrl: " + dbUrl);
            throw new RuntimeException("jdbc连接异常");
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        try {
            LOG.info("inputSplit = {}", inputSplit);
            hasNext = true;
            currentMessage = getMetaData(inputSplit);
        } catch (SQLException e) {
            errorMessage = setErrorMessage(e, "can not read data! current split: " + inputSplit.toString());
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

    /**
     * 如果程序正常执行，那么传递正常的数据，程序出现异常，则传递异常信息
     */
    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        row = new Row(1);
        if (filterData != null) {
            row.setField(0, objectMapper.writeValueAsString(currentMessage));
        } else {
            row.setField(0, objectMapper.writeValueAsString(errorMessage));
        }

        hasNext = false;

        tableColumn.clear();
        partitionColumn.clear();

        return row;
    }

    @Override
    protected void closeInternal() throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public void closeInputFormat() throws IOException {
        super.closeInputFormat();
        DBUtil.closeDBResources(resultSet, statement, connection, true);
    }

    public ResultSet executeSql(String sql) {
        ResultSet result;
        try {
            result = statement.executeQuery(sql);
        } catch (SQLException e) {
            errorMessage = setErrorMessage(e, "query error! current sql: " + sql);
            throw new RuntimeException(e);
        }
        return result;
    }

    public String buildDescSql(String table, boolean formatted) {
        String sql;
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
    public Map<String, Map<String, String>> transformDataToMap(ResultSet resultSet) {
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
            errorMessage = setErrorMessage(e, "transform data error");
            throw new RuntimeException("查询结果转化异常", e);
        }
        return result;
    }

    public Map<String, Object> getMetaData(InputSplit inputSplit) throws SQLException {
        Map<String, Object> map;
        queryTable = ((MetadataInputSplit) inputSplit).getTable();
        dbUrl = ((MetadataInputSplit) inputSplit).getDbUrl();
        resultSet = statement.executeQuery(buildDescSql(queryTable, true));

        filterData = transformDataToMap(resultSet);
        getTableColumns(queryTable);
        map = selectUsedData(filterData);
        return map;
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

        for (String item : tableColumn) {
            tempColumnList.add(setColumnMap(item, map.get(item), tableColumn.indexOf(item)));
        }
        result.put(MetaDataCons.KEY_COLUMN, tempColumnList);

        for (String item : partitionColumn) {
            tempPartitionColumnList.add(setColumnMap(item, map.get(item), partitionColumn.indexOf(item)));
        }
        result.put(MetaDataCons.KEY_PARTITION_COLUMN, tempPartitionColumnList);

        String storedType = null;
        if (map.get(MetaDataCons.KEY_INPUT_FORMAT).keySet().toString().contains(MetaDataCons.TYPE_TEXT)) {
            storedType = "text";
        }
        if (map.get(MetaDataCons.KEY_INPUT_FORMAT).keySet().toString().contains(MetaDataCons.TYPE_PARQUET)) {
            storedType = "Parquet";
        }

        result.put(MetaDataCons.KEY_TYPE, queryTable);

        temp.put(MetaDataCons.KEY_COMMENT, map.get("Table Parameters").get("comment"));
        temp.put(MetaDataCons.KEY_STORED_TYPE, storedType);
        result.put(MetaDataCons.KEY_TABLE_PROPERTIES, temp);

        // 全量读取为createType
        result.put(MetaDataCons.KEY_OPERA_TYPE, "createTable");
        return result;
    }

    /**
     * 获取查询表中字段名列表
     *
     * @param table
     * @return
     */
    public void getTableColumns(String table) {
        boolean isPartitionColumn = false;
        try {
            ResultSet temp = executeSql(buildDescSql(table, false));
            while (temp.next()) {
                if (temp.getString(1).trim().contains("Partition Information")) {
                    isPartitionColumn = true;
                }
                if (isPartitionColumn) {
                    partitionColumn.add(temp.getString(1).trim());
                } else {
                    tableColumn.add(temp.getString(1));
                }
            }

            // 除去多余的字段
            partitionColumn.remove("# Partition Information");
            partitionColumn.remove("");
            partitionColumn.remove("# col_name");
            tableColumn.remove("");

        } catch (SQLException e) {
            errorMessage = setErrorMessage(e, "can not get column");
            throw new RuntimeException("获取字段名列表异常");
        }
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
        result.put(MetaDataCons.KEY_COLUMN_NAME, columnName);
        if (map.keySet().toArray()[0] == null) {
            result.put(MetaDataCons.KEY_COLUMN_TYPE, map.keySet().toArray()[1]);
        } else {
            result.put(MetaDataCons.KEY_COLUMN_TYPE, map.keySet().toArray()[0]);
        }
        result.put(MetaDataCons.KEY_COLUMN_INDEX, index);
        result.put(MetaDataCons.KEY_COLUMN_COMMENT, map.values().toArray()[0]);
        return result;
    }

    /**
     * 生成任务异常信息map
     */
    public Map<String, String> setErrorMessage(Exception e, String message) {
        Map<String, String> map = new HashMap<>();
        map.put("error message", message);
        map.put(e.getClass().getSimpleName(), e.getMessage());
        return map;
    }
}
