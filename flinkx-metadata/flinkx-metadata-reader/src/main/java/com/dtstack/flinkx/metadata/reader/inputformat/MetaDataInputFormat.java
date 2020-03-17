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
package com.dtstack.flinkx.metadata.reader.inputformat;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.metadata.MetaDataCons;
import com.dtstack.flinkx.metadata.util.ConnUtil;
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
 * @date : 2020/3/8
 * @description : 元数据读取的抽象类
 */
public abstract class MetaDataInputFormat extends RichInputFormat {
    protected int numPartitions;
    protected Map<String, String> errorMessage = new HashMap<>();
    protected String dbUrl;
    protected List<String> table;
    protected String username;
    protected String password;

    protected List<Map> dbList;

    protected String currentQueryTable;

    protected List<String> tableList;

    protected Map<String, Object> currentMessage;

    protected boolean hasNext;

    protected boolean isAllDB;

    protected String driverName;

    protected Connection connection;
    protected Statement statement;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit = {}", inputSplit);
        currentMessage = new HashMap<>();
        currentQueryTable = ((MetaDataInputSplit) inputSplit).getTable();
        beforeUnit(currentQueryTable);
        currentMessage.put("data", unitMetaData(currentQueryTable));
        hasNext = true;

    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.createStatement();
        } catch (Exception e) {
            setErrorMessage(e, "connect error! current dbUrl" + dbUrl);
        }
    }

    @Override
    public void closeInputFormat() {
        try {
            super.closeInputFormat();
            ConnUtil.closeConn(null, statement, connection, true);
        } catch (Exception e) {
            setErrorMessage(e, "shut down resources error!");
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        initConnect();
        return initSplit();
    }

    /**
     * 创建分片方法
     *
     * @return 返回分片数组
     */
    protected InputSplit[] initSplit() throws SQLException {
        List<String> dbInfo;
        List<String> tableList = new ArrayList<>();
        if (isAllDB) {
            dbInfo = getDBList();
            tableList = getTableList(dbInfo);
        } else {
            for(Map item : dbList){
                if(item.get("tableList").equals(null)){
                    // 查询当前库下的所有表的元数据信息
                    tableList.addAll(getTableList((String) item.get("dbName")));
                } else{
                    List<String> tempList = (List<String>) item.get("tableList");
                    for(String table : tempList){
                        tableList.add(item.get("dbName") + "." + table);
                    }
                }
            }
        }
        table = tableList;
        int minNumSplits = tableList.size();
        InputSplit[] inputSplits = new MetaDataInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new MetaDataInputSplit(i, numPartitions, dbUrl, tableList.get(i));
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        row = new Row(1);
        if (errorMessage.isEmpty()) {
            currentMessage.put("querySuccess", true);
        } else {
            currentMessage.put("querySuccess", false);
        }
        currentMessage.put("errorMsg", errorMessage);
        row.setField(0, objectMapper.writeValueAsString(currentMessage));
        hasNext = false;

        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    /**
     * 任务异常信息
     */
    protected void setErrorMessage(Exception e, String message) {
        if (errorMessage.isEmpty()) {
            errorMessage.put("errorMessage", message + " detail:" + e.getMessage());
        }
    }

    /**
     * 执行查询计划
     */
    protected ResultSet executeSql(String sql) {
        ResultSet result = null;
        try {
            result = statement.executeQuery(sql);
        } catch (SQLException e) {
            setErrorMessage(e, "query error! current sql: " + sql);
        }
        return result;
    }

    /**
     * 组合元数据信息之前的操作，比如hive2的获取字段名和分区字段
     *
     * @param currentQueryTable 当前查询的table
     */
    protected abstract void beforeUnit(String currentQueryTable);

    /**
     * 从结果集中解析有关表的元数据信息
     *
     * @param currentQueryTable 当前查询的table名
     * @return 有关表的元数据信息
     */
    public abstract Map<String, Object> getTablePropertites(String currentQueryTable);

    /**
     * 从结果集中解析有关表字段的元数据信息
     *
     * @return 有关表字段的元数据信息
     */
    public abstract Map<String, Object> getColumnPropertites(String currentQueryTable);

    /**
     * 从结果集中解析有关分区字段的元数据信息
     *
     * @return 有关分区字段的元数据信息
     */
    public abstract Map<String, Object> getPartitionPropertites(String currentQueryTable);

    /**
     * 构建查询表名sql，如show tables
     *
     * @return 返回能够查询tables的sql语句
     */
    public abstract String queryTableSql();

    /**
     * 构建查询当前连接下可查询的所有数据库名，如show databases;
     *
     * @return 返回能够查询databases的sql语句
     */
    public abstract String queryDBSql();

    /**
     * 构建切换databases的执行语句，如use default;
     * @param dbName
     * @return
     */
    public abstract String changeDBSql(String dbName);

    /**
     * 对元数据信息整合
     */
    public Map<String, Object> unitMetaData(String currentQueryTable) {
        Map<String, Object> tablePropertities = getTablePropertites(currentQueryTable);
        Map<String, Object> columnPreportities = getColumnPropertites(currentQueryTable);
        Map<String, Object> partitionPreprotities = getPartitionPropertites(currentQueryTable);
        Map<String, Object> result = new HashMap<>();
        result.put("dbTypeAndVersion", "");
        result.put(MetaDataCons.KEY_TABLE, currentQueryTable);
        result.put(MetaDataCons.KEY_OPERA_TYPE, "createTable");

        if (!columnPreportities.isEmpty()) {
            result.put(MetaDataCons.KEY_COLUMN, columnPreportities);
        }

        if (!tablePropertities.isEmpty()) {
            result.put(MetaDataCons.KEY_TABLE_PROPERTITES, tablePropertities);
        }

        if (!partitionPreprotities.isEmpty()) {
            result.put(MetaDataCons.KEY_PARTITION_PROPERTITES, partitionPreprotities);
        }

        return result;
    }

    public List<String> getTableList(String dbName) throws SQLException {
        List<String> tableList = new ArrayList<>();
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(queryTableSql());
        while (resultSet.next()) {
            tableList.add(dbName + "." + resultSet.getString(1));
        }
        return tableList;
    }

    public List<String> getTableList(List<String> dbList) throws SQLException {
        List<String> tableList = new ArrayList<>();
        for (String item : dbList) {
            statement.execute(changeDBSql(item));
            tableList.addAll(getTableList(item));
        }
        return tableList;
    }

    /**
     * 如果isAllDB为true，那么通过 show databases获取全部的dbName
     */
    public List<String> getDBList() throws SQLException {
        List<String> result = new ArrayList<>();
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(queryDBSql());
        while (resultSet.next()) {
            result.add(resultSet.getString(1));
        }
        return result;
    }

    public void initConnect() {
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection(dbUrl, username, password);
        } catch (Exception e) {
            setErrorMessage(e, "init connect error");
        }
    }
}
