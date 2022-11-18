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

package com.dtstack.chunjun.restore.mysql;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.config.CacheConfig;
import com.dtstack.chunjun.cdc.ddl.DdlRowDataConvented;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.cdc.handler.CacheHandler;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.restore.mysql.datasource.DruidDataSourceManager;
import com.dtstack.chunjun.restore.mysql.transformer.ColumnRowDataTransformer;
import com.dtstack.chunjun.restore.mysql.transformer.DDLRowDataTransformer;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.table.data.RowData;

import com.google.common.collect.Queues;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_DRIVERCLASSNAME;
import static com.dtstack.chunjun.constants.CDCConstantValue.LSN;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.DATABASE_KEY;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.DELETE_PROCESSED_CACHE;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.DELETE_PROCESSED_CACHE_DATABASE_NULLABLE;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.SELECT_CACHE;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.SELECT_CACHE_DATABASE_NULLABLE;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.SEND_CACHE;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.TABLE_KEY;

public class MysqlCacheHandler extends CacheHandler {

    private static final long serialVersionUID = -6787359265687998728L;

    private static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

    private transient DataSource dataSource;

    private transient Connection connection;

    private transient PreparedStatement sendCacheStatement;

    private transient PreparedStatement selectCacheStatement;

    private transient PreparedStatement selectCacheStatementDataBaseNullAble;

    private transient PreparedStatement deleteCacheStatement;

    private transient PreparedStatement deleteCacheStatementDataBaseNullAble;

    public MysqlCacheHandler(CacheConfig cacheConfig) {
        super(cacheConfig);
    }

    @Override
    public void init(Properties properties) throws Exception {
        properties.put(PROP_DRIVERCLASSNAME, MYSQL_DRIVER_NAME);

        String database = properties.getProperty(DATABASE_KEY);
        String table = properties.getProperty(TABLE_KEY);

        this.dataSource = DruidDataSourceManager.create(properties);
        this.connection = dataSource.getConnection();

        String sendCache = SEND_CACHE.replace("$database", database).replace("$table", table);
        String selectCache = SELECT_CACHE.replace("$database", database).replace("$table", table);
        String selectCacheDataBaseNullAble =
                SELECT_CACHE_DATABASE_NULLABLE
                        .replace("$database", database)
                        .replace("$table", table);
        String deleteCache =
                DELETE_PROCESSED_CACHE.replace("$database", database).replace("$table", table);
        String deleteCacheDatBaseNullAble =
                DELETE_PROCESSED_CACHE_DATABASE_NULLABLE
                        .replace("$database", database)
                        .replace("$table", table);
        sendCacheStatement = connection.prepareStatement(sendCache);
        selectCacheStatement = connection.prepareStatement(selectCache);
        selectCacheStatementDataBaseNullAble =
                connection.prepareStatement(selectCacheDataBaseNullAble);
        deleteCacheStatement = connection.prepareStatement(deleteCache);
        deleteCacheStatementDataBaseNullAble =
                connection.prepareStatement(deleteCacheDatBaseNullAble);
    }

    @Override
    public void shutdown() throws SQLException {
        if (null != dataSource && null != connection) {
            connection.close();
        }
    }

    @Override
    public boolean sendCache(Collection<RowData> data, TableIdentifier tableIdentifier) {
        AtomicBoolean isCached = new AtomicBoolean(false);
        try {
            for (RowData rowData : data) {
                if (rowData instanceof DdlRowData) {
                    DdlRowData ddlRowData = (DdlRowData) rowData;
                    String operationType = ddlRowData.getType().name();
                    String lsn = ddlRowData.getLsn();
                    String content = GSON.toJson(ddlRowData);
                    Integer lsnSequence = ddlRowData.getLsnSequence();

                    sendCacheStatement.setString(1, tableIdentifier.getDataBase());
                    sendCacheStatement.setString(2, tableIdentifier.getSchema());
                    sendCacheStatement.setString(3, tableIdentifier.getTable());
                    sendCacheStatement.setString(4, operationType);
                    sendCacheStatement.setString(5, lsn);
                    sendCacheStatement.setInt(6, lsnSequence);

                    if (rowData instanceof DdlRowDataConvented
                            && ((DdlRowDataConvented) rowData).conventSuccessful()) {
                        sendCacheStatement.setString(7, content);
                    } else {
                        sendCacheStatement.setString(7, content);
                    }
                }

                if (rowData instanceof ColumnRowData) {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;

                    String operationType = columnRowData.getRowKind().name();
                    String content = GSON.toJson(columnRowData);

                    sendCacheStatement.setString(1, tableIdentifier.getDataBase());
                    sendCacheStatement.setString(2, tableIdentifier.getSchema());
                    sendCacheStatement.setString(3, tableIdentifier.getTable());
                    sendCacheStatement.setString(4, operationType);
                    sendCacheStatement.setString(
                            5, Objects.requireNonNull(columnRowData.getField(LSN)).asString());

                    sendCacheStatement.setInt(6, 0);
                    sendCacheStatement.setString(7, content);
                }

                sendCacheStatement.addBatch();
            }
            sendCacheStatement.executeBatch();
            isCached.set(true);
        } catch (SQLException e) {
            // TODO 异常优化
            throw new ChunJunRuntimeException("Can not insert cache data.", e);
        }
        return isCached.get();
    }

    @Override
    public Queue<RowData> fromCache(TableIdentifier tableIdentifier) {
        int batchSize = cacheConfig.getCacheSize();
        Queue<RowData> queue = Queues.newLinkedBlockingQueue(batchSize);
        try {
            ResultSet resultSet;
            if (tableIdentifier.getDataBase() != null) {
                selectCacheStatement.setString(1, tableIdentifier.getDataBase());
                selectCacheStatement.setString(2, tableIdentifier.getSchema());
                selectCacheStatement.setString(3, tableIdentifier.getTable());
                selectCacheStatement.setInt(4, batchSize);

                resultSet = selectCacheStatement.executeQuery();
            } else {
                selectCacheStatementDataBaseNullAble.setString(1, tableIdentifier.getSchema());
                selectCacheStatementDataBaseNullAble.setString(2, tableIdentifier.getTable());
                selectCacheStatementDataBaseNullAble.setInt(3, batchSize);

                resultSet = selectCacheStatementDataBaseNullAble.executeQuery();
            }
            String lastLsn = "";
            int lastLsnSequence = 0;
            while (resultSet.next()) {
                String content = resultSet.getString("content");
                if (content.contains("ddlInfos")) {
                    DdlRowData transform = DDLRowDataTransformer.transform(content);
                    queue.add(transform);
                    lastLsn = transform.getLsn();
                    lastLsnSequence = transform.getLsnSequence();
                } else {
                    ColumnRowData transform = ColumnRowDataTransformer.transform(content);
                    queue.add(transform);
                    lastLsn = Objects.requireNonNull(transform.getField("lsn")).asString();
                    lastLsnSequence = 0;
                }
            }
            deleteCache(tableIdentifier, lastLsn, lastLsnSequence);
            return queue;
        } catch (SQLException e) {
            throw new ChunJunRuntimeException("Can not select cache of " + tableIdentifier, e);
        }
    }

    @Override
    public void deleteCache(TableIdentifier tableIdentifier, String lsn, int lsnSequence) {
        try {
            if (null != tableIdentifier.getDataBase()) {
                deleteCacheStatement.setString(1, tableIdentifier.getDataBase());
                deleteCacheStatement.setString(2, tableIdentifier.getSchema());
                deleteCacheStatement.setString(3, tableIdentifier.getTable());
                deleteCacheStatement.setString(4, lsn);
                deleteCacheStatement.setString(5, lsn);
                deleteCacheStatement.setInt(6, lsnSequence);
                deleteCacheStatement.executeUpdate();
            } else {

                deleteCacheStatementDataBaseNullAble.setString(1, tableIdentifier.getSchema());
                deleteCacheStatementDataBaseNullAble.setString(2, tableIdentifier.getTable());
                deleteCacheStatementDataBaseNullAble.setString(3, lsn);
                deleteCacheStatementDataBaseNullAble.setString(4, lsn);
                deleteCacheStatementDataBaseNullAble.setInt(5, lsnSequence);
                deleteCacheStatementDataBaseNullAble.executeUpdate();
            }

        } catch (SQLException e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "delete cache data failed, tableIdentifier: [%s], lsn: [%s], lsn_sequence: [%s]",
                            tableIdentifier, lsn, lsnSequence),
                    e);
        }
    }
}
