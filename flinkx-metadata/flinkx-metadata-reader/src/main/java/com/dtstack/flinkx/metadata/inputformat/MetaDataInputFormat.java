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
package com.dtstack.flinkx.metadata.inputformat;

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
 * @description : 元数据同步的抽象类
 */
public abstract class MetaDataInputFormat extends RichInputFormat {
    protected int numPartitions;
    protected String dbUrl;
    protected String username;
    protected String password;

    protected List<Map> dbList;

    protected Map<String, String> errorMessage = new HashMap<>();
    protected Map<String, Object> currentMessage = new HashMap<>();

    protected boolean hasNext;

    protected boolean isAllDB;

    protected String driverName;

    protected transient Connection connection;
    protected transient Statement statement;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit = {}", inputSplit);
        String currentDbName = ((MetaDataInputSplit) inputSplit).getDbName();
        String currentQueryTable = ((MetaDataInputSplit) inputSplit).getTableName();
        beforeUnit(currentQueryTable, currentDbName);
        currentMessage.put("data", unitMetaData(currentQueryTable, currentDbName));
        hasNext = true;

    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        initConnect();
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
            dbInfo = getDataList(queryDBSql());
            tableList = getTableList(dbInfo);
        } else {
            for (Map item : dbList) {
                if (item.get(MetaDataCons.KEY_TABLE_LIST).equals(null)) {
                    // 查询当前库下的所有表的元数据信息
                    tableList.addAll(getTableList((String) item.get(MetaDataCons.KEY_DB_NAME)));
                } else {
                    List<String> tempList = (List<String>) item.get(MetaDataCons.KEY_TABLE_LIST);
                    for (String table : tempList) {
                        tableList.add(item.get(MetaDataCons.KEY_DB_NAME) + "." + table);
                    }
                }
            }
        }
        int minNumSplits = tableList.size();
        InputSplit[] inputSplits = new MetaDataInputSplit[minNumSplits];
        for (int index = 0; index < minNumSplits; index++) {
            String dbName = tableList.get(index).split("\\.")[0];
            String tableName = tableList.get(index).split("\\.")[1];
            inputSplits[index] = new MetaDataInputSplit(index, numPartitions, dbUrl, tableName, dbName);
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
        errorMessage.clear();

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
        LOG.info("current query sql : {}", sql);
        try {
            result = statement.executeQuery(sql);
        } catch (SQLException e) {
            LOG.error("query error! current query sql:" + sql, e);
            setErrorMessage(e, "query error! current sql: " + sql);
        }
        return result;
    }

    /**
     * 组合元数据信息之前的操作，比如hive2的获取字段名和分区字段
     *
     * @param currentQueryTable 当前查询的table
     * @param currentDbName 当前查询的db
     */
    protected abstract void beforeUnit(String currentQueryTable, String currentDbName);

    /**
     * 从结果集中解析有关表的元数据信息
     *
     * @param currentQueryTable 当前查询的table名
     * @param currentDbName 当前查询的db
     * @return 有关表的元数据信息
     */
    public abstract Map<String, Object> getTablePropertites(String currentQueryTable, String currentDbName);

    /**
     * 从结果集中解析有关表字段的元数据信息
     * @param currentQueryTable 当前查询的table
     * @return 有关表字段的元数据信息
     */
    public abstract Map<String, Object> getColumnPropertites(String currentQueryTable);

    /**
     * 从结果集中解析有关分区字段的元数据信息
     *
     * @return 有关分区字段的元数据信息
     */
    public abstract Map<String, Object> getPartitionPropertites(String currentQueryTable, String currentDbName);

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
     * @param dbName 需要切换的数据库名称
     * @return 返回能够切换数据库的sql,如use default
     */
    public abstract String changeDBSql(String dbName);

    public abstract String getStartQuote();

    public abstract String getEndQuote();

    public abstract String quoteData(String data);

    /**
     * 对元数据信息整合
     */
    public Map<String, Object> unitMetaData(String currentQueryTable, String currentDbName) {
        Map<String, Object> tableDetailInformation = getTablePropertites(currentQueryTable, currentDbName);
        Map<String, Object> columnDetailInformation = getColumnPropertites(currentQueryTable);
        Map<String, Object> partitionDetailInformation = getPartitionPropertites(currentQueryTable, currentDbName);
        Map<String, Object> result = new HashMap<>();
        result.put(MetaDataCons.KEY_TABLE, currentQueryTable);
        result.put(MetaDataCons.KEY_SCHEMA, currentDbName);
        // 当前为全量查询，所以type固定为createTable
        result.put(MetaDataCons.KEY_OPERA_TYPE, "createTable");

        if (!columnDetailInformation.isEmpty()) {
            result.put(MetaDataCons.KEY_COLUMN, columnDetailInformation);
        }

        if (!tableDetailInformation.isEmpty()) {
            result.put(MetaDataCons.KEY_TABLE_PROPERTITES, tableDetailInformation);
        }

        if (!partitionDetailInformation.isEmpty()) {
            result.put(MetaDataCons.KEY_PARTITION_PROPERTITES, partitionDetailInformation);
        }

        return result;
    }

    public List<String> getTableList(List<String> dbList) throws SQLException {
        List<String> tableList = new ArrayList<>();
        for (String item : dbList) {
            statement.execute(changeDBSql(item));
            tableList.addAll(getTableList(item));
        }
        return tableList;
    }

    public List<String> getTableList(String dbName) throws SQLException {
        List<String> tableList = new ArrayList<>();
        ResultSet resultSet = executeSql(queryTableSql());
        while (resultSet.next()) {
            tableList.add(dbName + "." + resultSet.getString(1));
        }
        return tableList;
    }

    /**
     * 如果isAllDB为true，那么通过 show databases获取全部的dbName
     */
    public List<String> getDataList(String queryDataSql) throws SQLException{
        List<String> result = new ArrayList<>();
        ResultSet resultSet = executeSql(queryDataSql);
        while (resultSet.next()) {
            result.add(resultSet.getString(1));
        }
        return result;
    }

    public void initConnect() {
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.createStatement();
        } catch (Exception e) {
            setErrorMessage(e, "init connect error");
        }
    }
}
