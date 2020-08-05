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

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.metadata.MetaDataCons;
import com.dtstack.flinkx.metadata.util.ConnUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/20
 */
public abstract class BaseMetadataInputFormat extends BaseRichInputFormat {

    protected String dbUrl;

    protected String username;

    protected String password;

    protected String driverName;

    protected List<Map<String, Object>> dbTableList;

    protected static transient ThreadLocal<Connection> connection = new ThreadLocal<>();

    protected static transient ThreadLocal<Statement> statement = new ThreadLocal<>();

    protected static transient ThreadLocal<String> currentDb = new ThreadLocal<>();

    protected transient Iterator<String> tableIterator;

    protected MetadataDbTableList metadataDbTableList;

    protected int totalTable = 0;

    protected int resolvedTable = 0;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        try {
            connection.set(getConnection());
            statement.set(connection.get().createStatement());
        } catch (Exception e) {
            LOG.error("获取连接失败, dbUrl = {}, username = {}, e = {}", dbUrl, username, ExceptionUtil.getErrorMessage(e));
            throw new IOException("获取连接失败", e);
        }
        initDbList();
    }

    public void initDbList() throws IOException {
        try{
            //同步所有库的所有表
            if (CollectionUtils.isEmpty(dbTableList)) {
                List<String> dbList = showDatabases();
                dbTableList = new ArrayList<>();
                for (String s : dbList) {
                    Map<String, Object> dbTables = new HashMap<>(4);
                    dbTables.put(MetaDataCons.KEY_DB_NAME, s);
                    switchDatabase(s);
                    List<String> tableList = showTables();
                    dbTables.put(MetaDataCons.KEY_TABLE_LIST, tableList);
                    dbTableList.add(dbTables);
                    totalTable += tableList.size();
                }
            } else {
                for (int index = 0; index < dbTableList.size(); index++) {
                    Map<String, Object> dbTables = dbTableList.get(index);
                    String dbName = MapUtils.getString(dbTables, MetaDataCons.KEY_DB_NAME);
                    List<String> tableList = (List<String>) dbTables.get(MetaDataCons.KEY_TABLE_LIST);
                    //同步一个库里所有表
                    if(CollectionUtils.isEmpty(tableList)){
                        switchDatabase(dbName);
                        tableList = showTables();
                        dbTables.put(MetaDataCons.KEY_TABLE_LIST, tableList);
                        dbTableList.set(index, dbTables);
                    }
                    totalTable += tableList.size();
                }
            }
        }catch (SQLException e){
            LOG.error(ExceptionUtils.getMessage(e));
            throw new IOException(e.getCause());
        }
        metadataDbTableList = new MetadataDbTableList(dbTableList);
    }

    protected void initProperty() throws SQLException {
        currentDb.set(metadataDbTableList.getDbName());
        switchDatabase(currentDb.get());
        List<String> tableList = metadataDbTableList.getTableList();
        tableIterator = tableList.iterator();
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        try {
            initProperty();
        } catch (SQLException e) {
            LOG.error("获取table列表异常, dbUrl = {}, username = {}, inputSplit = {}, e = {}", dbUrl, username, inputSplit, ExceptionUtil.getErrorMessage(e));
            throw new IOException("获取table列表异常", e);
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int splitNumber) {
        InputSplit[] inputSplits = new InputSplit[splitNumber];
        for (int i = 0; i < splitNumber; i++) {
            inputSplits[i] = new GenericInputSplit(i, splitNumber);
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) {
        Map<String, Object> metaData = new HashMap<>(16);
        metaData.put(MetaDataCons.KEY_OPERA_TYPE, MetaDataCons.DEFAULT_OPERA_TYPE);

        String tableName = tableIterator.next();
        metaData.put(MetaDataCons.KEY_SCHEMA, currentDb.get());
        metaData.put(MetaDataCons.KEY_TABLE, tableName);
        metaData.put(MetaDataCons.KEY_TOTAL_TABLE, totalTable);
        metaData.put(MetaDataCons.KEY_RESOLVED_TABLE, ++resolvedTable);

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
    public boolean reachedEnd() throws IOException {
        if(!tableIterator.hasNext()){
            metadataDbTableList.increPosition();
            if(metadataDbTableList.reachEndPosition()){
                return true;
            }else {
                try {
                    initProperty();
                }catch (SQLException e){
                    LOG.error(ExceptionUtil.getErrorMessage(e));
                    throw new IOException(e.getCause());
                }
                return false;
            }
        }else {
            return false;
        }
    }

    @Override
    protected void closeInternal() throws IOException {
        Statement st = statement.get();
        if (null != st) {
            try {
                st.close();
                statement.remove();
            } catch (SQLException e) {
                LOG.error("关闭statement对象异常, e = {}", ExceptionUtil.getErrorMessage(e));
                throw new IOException("关闭statement对象异常", e);
            }
        }

        currentDb.remove();
    }

    @Override
    public void closeInputFormat() throws IOException {
        super.closeInputFormat();
        Connection conn = connection.get();
        if (null != conn) {
            try {
                conn.close();
                connection.remove();
            } catch (SQLException e) {
                LOG.error("关闭数据库连接异常, e = {}", ExceptionUtil.getErrorMessage(e));
                throw new IOException("关闭数据库连接异常", e);
            }
        }
    }

    /**
     * 创建数据库连接
     */
    public Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        return ConnUtil.getConnection(dbUrl, username, password);
    }

    /**
     * 查询所有database名称
     *
     * @return database名称列表
     * @throws SQLException e
     */
    protected List<String> showDatabases() throws SQLException {
        List<String> dbNameList = new ArrayList<>();
        try(ResultSet rs = statement.get().executeQuery(MetaDataCons.SQL_SHOW_DATABASES)) {
            while (rs.next()) {
                dbNameList.add(rs.getString(1));
            }
        }

        return dbNameList;
    }

    /**
     * 查询当前数据库下所有的表
     *
     * @return  表名列表
     * @throws SQLException SQL异常
     */
    protected abstract List<String> showTables() throws SQLException;

    /**
     * 切换当前database
     *
     * @param databaseName 数据库名
     * @throws SQLException SQL异常
     */
    protected abstract void switchDatabase(String databaseName) throws SQLException;

    /**
     * 根据表名查询元数据信息
     * @param tableName 表名
     * @return 元数据信息
     * @throws SQLException SQL异常
     */
    protected abstract Map<String, Object> queryMetaData(String tableName) throws SQLException;

    /**
     * 将数据库名，表名，列名字符串转为对应的引用，如：testTable -> `testTable`
     * @param name 入参
     * @return 返回数据库名，表名，列名的引用
     */
    protected abstract String quote(String name);
}
