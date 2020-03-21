/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
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
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/20
 */
public abstract class MetadataInputFormat extends RichInputFormat {
    protected int numPartitions;
    protected String dbUrl;
    protected String username;
    protected String password;
    protected String driverName;

    protected List<Map<String, Object>> dbList;

    protected boolean hasNext;

    protected boolean isAll;

    protected transient static Connection connection;
    protected transient static Statement statement;

    protected Map<String, String> errorMessage;
    protected Map<String, Object> currentMessage;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        errorMessage = new HashMap<>();
        currentMessage = new HashMap<>();
        initConnect();
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit = {}", inputSplit);
        String currentDb = ((MetadataInputSplit) inputSplit).getDbName();
        List<String> tableList = ((MetadataInputSplit) inputSplit).getTableList();
        List<Map<String, Object>> resultMapList = new ArrayList<>();
        try {
            // 切换数据库，获取当前数据库下的元数据信息
            statement.execute(changeDbSql(currentDb));
            if(tableList.isEmpty()){
                tableList.addAll(getDataList(queryTableSql()));
            }
            for(String currentTable : tableList){
                beforeUnit(currentTable, currentDb);

                Map<String, Object> resultMap = unitMetaData(currentDb, currentTable);

                if (errorMessage.isEmpty()) {
                    resultMap.put("querySuccess", true);
                } else {
                    resultMap.put("querySuccess", false);
                }
                resultMap.put("errorMsg", errorMessage);
                hasNext = true;
                resultMapList.add(resultMap);
            }
            currentMessage.put("data", resultMapList);
        } catch (SQLException e) {
            setErrorMessage(e, "openInternal error");
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int i) throws Exception {
        initConnect();
        return initSplits();
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return null;
    }

    @Override
    protected void closeInternal() throws IOException {
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    /**
     * 创建分片
     */
    private InputSplit[] initSplits() throws SQLException {
        List<String> dbInfo = new ArrayList<>();
        List<List<String>> tableList = new ArrayList<>();
        if (isAll) {
            dbInfo.addAll(new ArrayList<>(getDataList(queryDbSql())));
        } else {
            for (Map<String, Object> item : dbList) {
                dbInfo.add(item.get(MetaDataCons.KEY_DB_NAME).toString());
                tableList.add((List<String>) item.get(MetaDataCons.KEY_TABLE_LIST));
            }
        }
        InputSplit[] inputSplits = new MetadataInputSplit[dbInfo.size()];
        for (int index = 0; index < dbInfo.size(); index++) {
            inputSplits[index] = new MetadataInputSplit(index, numPartitions, dbInfo.get(index), tableList.get(index));
        }
        return inputSplits;
    }

    /**
     * 执行查询语句，例如查询库名、表名
     *
     * @param queryDataSql 查询sql语句
     * @return result 查询结果拆分之后的数据
     */
    public List<String> getDataList(String queryDataSql) throws SQLException {
        List<String> result = new ArrayList<>();
        ResultSet resultSet = executeQuerySql(queryDataSql);
        while (resultSet.next()) {
            result.add(resultSet.getString(1));
        }
        return result;
    }

    /**
     * 执行查询计划
     */
    protected ResultSet executeQuerySql(String sql) {
        LOG.info("current query sql : {}", sql);
        try {
            return statement.executeQuery(sql);
        } catch (SQLException e) {
            LOG.warn("query error! current query sql:" + sql, e);
            setErrorMessage(e, "query error! current sql: " + sql);
        }
        return null;
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
     * 初始化连接
     */
    public void initConnect() {
        try {
            Class.forName(driverName);
            connection = ConnUtil.getConnection(dbUrl, username, password);
            statement = connection.createStatement();
        } catch (Exception e) {
            throw new RuntimeException("get connect error!", e);
        }
    }


    protected  Map<String, Object> unitMetaData(String currentDb, String currentTable){
        Map<String, Object> tableDetailInformation = getTablePropertites(currentTable, currentDb);
        List<Map<String, Object>> columnDetailInformation = getColumnPropertites(currentTable);
        List<Map<String, Object>> partitionDetailInformation = getPartitionPropertites(currentTable, currentDb);
        Map<String, Object> result = new HashMap<>();
        result.put(MetaDataCons.KEY_TABLE, currentTable);
        result.put(MetaDataCons.KEY_SCHEMA, currentDb);
        // 当前为全量查询，所以type固定为createTable
        result.put(MetaDataCons.KEY_OPERA_TYPE, "createTable");

        result.put(MetaDataCons.KEY_COLUMN, columnDetailInformation);

        result.put(MetaDataCons.KEY_TABLE_PROPERTITES, tableDetailInformation);

        result.put(MetaDataCons.KEY_PARTITION_PROPERTITES, partitionDetailInformation);

        return result;
    }

    /**
     * 组合元数据信息之前的操作，比如hive2的获取字段名和分区字段
     *
     * @param currentQueryTable 当前查询的table
     * @param currentDbName     当前查询的db
     */
    protected abstract void beforeUnit(String currentQueryTable, String currentDbName);

    /**
     * 从结果集中解析有关表的元数据信息
     *
     * @param currentQueryTable 当前查询的table名
     * @param currentDbName     当前查询的db
     * @return 有关表的元数据信息
     */
    public abstract Map<String, Object> getTablePropertites(String currentQueryTable, String currentDbName);

    /**
     * 从结果集中解析有关表字段的元数据信息
     *
     * @param currentQueryTable 当前查询的table
     * @return 有关表字段的元数据信息
     */
    public abstract List<Map<String, Object>> getColumnPropertites(String currentQueryTable);

    /**
     * 从结果集中解析有关分区字段的元数据信息
     *
     * @return 有关分区字段的元数据信息
     */
    public abstract List<Map<String, Object>> getPartitionPropertites(String currentQueryTable, String currentDbName);

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
    public abstract String queryDbSql();

    /**
     * 构建切换databases的执行语句，如use default;
     *
     * @param dbName 需要切换的数据库名称
     * @return 返回能够切换数据库的sql, 如use default
     */
    public abstract String changeDbSql(String dbName);

    public abstract String getStartQuote();

    public abstract String getEndQuote();

    public abstract String quoteData(String data);
}
