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
import com.dtstack.flinkx.metadata.util.ConnUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/20
 */
public abstract class BaseMetadataInputFormat extends RichInputFormat {

    protected String dbUrl;

    protected String username;

    protected String password;

    protected String driverName;

    protected List<Map<String, Object>> dbTableList;

    protected transient static Connection connection;

    protected transient static Statement statement;

    protected String currentDb;

    protected transient Iterator<String> tableIterator;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        try {
            connection = getConnection();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit = {}", inputSplit);

        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            throw new IOException("create statement error", e);
        }

        try {
            currentDb = ((MetadataInputSplit) inputSplit).getDbName();
            switchDatabase(currentDb);
        } catch (Exception e) {
            throw new IOException("switch database error", e);
        }

        List<String> tableList = ((MetadataInputSplit) inputSplit).getTableList();
        if (CollectionUtils.isEmpty(tableList)) {
            try {
                tableList = showTables();
            } catch (SQLException e) {
                throw new IOException("show tables error", e);
            }
        }

        tableIterator = tableList.iterator();
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int splitNumber) throws Exception {
        if (CollectionUtils.isEmpty(dbTableList)) {
            try (Connection connection = getConnection()) {
                List<String> dbList = showDatabases(connection);
                InputSplit[] inputSplits = new MetadataInputSplit[dbList.size()];
                for (int i = 0; i < dbList.size(); i++) {
                    inputSplits[i] = new MetadataInputSplit(splitNumber, dbList.get(i), null);
                }

                return inputSplits;
            }
        } else {
            InputSplit[] inputSplits = new MetadataInputSplit[dbTableList.size()];
            for (int index = 0; index < dbTableList.size(); index++) {
                Map<String, Object> dbTables = dbTableList.get(index);
                String dbName = MapUtils.getString(dbTables, "dbName");
                if(StringUtils.isNotEmpty(dbName)){
                    List<String> tables = (List<String>)dbTables.get("tableList");
                    inputSplits[index] = new MetadataInputSplit(splitNumber, dbName, tables);
                }
            }
            return inputSplits;
        }
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        Map<String, Object> metaData = new HashMap<>();
        metaData.put("operaType", "createTable");

        try {
            String tableName = tableIterator.next();
            metaData.putAll(queryMetaData(tableName));
            metaData.put("querySuccess", true);
        } catch (Exception e) {
            metaData.put("querySuccess", false);
            metaData.put("errorMsg", ExceptionUtil.getErrorMessage(e));
        }

        return Row.of(metaData);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !tableIterator.hasNext();
    }

    @Override
    protected void closeInternal() throws IOException {
        if (null != statement) {
            try {
                statement.close();
                statement = null;
            } catch (SQLException e) {
                throw new IOException("close statement error", e);
            }
        }
    }

    @Override
    public void closeInputFormat() throws IOException {
        super.closeInputFormat();

        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("close connection error", e);
            }
        }
    }

    /**
     * 创建连接
     */
    public Connection getConnection() throws Exception{
        Class.forName(driverName);
        return ConnUtil.getConnection(dbUrl, username, password);
    }

    /**
     * query all databases
     *
     * @param connection jdbc connection
     * @return db list
     * @throws SQLException e
     */
    protected abstract List<String> showDatabases(Connection connection) throws SQLException;

    /**
     *
     * @return
     * @throws SQLException
     */
    protected abstract List<String> showTables() throws SQLException;

    /**
     * switch db
     *
     * @param database
     * @throws SQLException
     */
    protected abstract void switchDatabase(String database) throws SQLException;

    /**
     * query metadata
     * @param table
     * @return
     * @throws SQLException
     */
    protected abstract Map<String, Object> queryMetaData(String table) throws SQLException;

    /**
     * quote database,table,column
     * @param value
     * @return
     */
    protected abstract String quote(String value);
}
