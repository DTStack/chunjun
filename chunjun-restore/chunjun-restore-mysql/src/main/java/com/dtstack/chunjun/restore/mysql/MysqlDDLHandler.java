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
import com.dtstack.chunjun.cdc.config.DDLConfig;
import com.dtstack.chunjun.cdc.ddl.DdlRowDataConvented;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.cdc.handler.DDLHandler;
import com.dtstack.chunjun.restore.mysql.datasource.DruidDataSourceManager;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_DRIVERCLASSNAME;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.DATABASE_KEY;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.DATABASE_NAME_KEY;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.DELETE_CHANGED_DDL;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.DELETE_CHANGED_DDL_DATABASE_NULLABLE;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.FIND_DDL_CHANGED;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.FIND_DDL_UNCHANGED;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.INSERT_DDL_CHANGE;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.SCHEMA_NAME_KEY;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.TABLE_KEY;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.TABLE_NAME_KEY;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.UPDATE_DDL_CHANGE;
import static com.dtstack.chunjun.restore.mysql.constant.SqlConstants.UPDATE_DDL_CHANGE_DATABASE_NULLABLE;

@Slf4j
public class MysqlDDLHandler extends DDLHandler {

    private static final long serialVersionUID = -282909536908351475L;

    private static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

    private transient Connection connection;

    private transient PreparedStatement findDDLChangeStatement;

    private transient PreparedStatement insertDDLChangeStatement;

    private transient PreparedStatement findDDLUnchangedStatement;

    private transient PreparedStatement updateDDLChangeStatement;

    private transient PreparedStatement updateDDLChangeDataBaseNullAbleStatement;

    private transient PreparedStatement deleteChangedDDLStatement;
    private transient PreparedStatement deleteChangedDDLStatementDataBaseNullAbleStatement;

    public MysqlDDLHandler(DDLConfig ddlConfig) {
        super(ddlConfig);
    }

    @Override
    public void init(Properties properties) throws Exception {

        properties.put(PROP_DRIVERCLASSNAME, MYSQL_DRIVER_NAME);

        String database = properties.getProperty(DATABASE_KEY);
        String table = properties.getProperty(TABLE_KEY);

        DataSource dataSource = DruidDataSourceManager.create(properties);
        this.connection = dataSource.getConnection();

        findDDLChangeStatement = prepare(FIND_DDL_CHANGED, database, table, connection);
        findDDLUnchangedStatement = prepare(FIND_DDL_UNCHANGED, database, table, connection);
        insertDDLChangeStatement = prepare(INSERT_DDL_CHANGE, database, table, connection);
        updateDDLChangeStatement = prepare(UPDATE_DDL_CHANGE, database, table, connection);
        updateDDLChangeDataBaseNullAbleStatement =
                prepare(UPDATE_DDL_CHANGE_DATABASE_NULLABLE, database, table, connection);
        deleteChangedDDLStatement = prepare(DELETE_CHANGED_DDL, database, table, connection);
        deleteChangedDDLStatementDataBaseNullAbleStatement =
                prepare(DELETE_CHANGED_DDL_DATABASE_NULLABLE, database, table, connection);
    }

    private PreparedStatement prepare(
            String sqlTemplate, String database, String table, Connection connection)
            throws SQLException {
        String replace = sqlTemplate.replace("$database", database).replace("$table", table);
        return connection.prepareStatement(replace);
    }

    @Override
    protected void shutdown() throws Exception {
        if (null != connection) {
            connection.close();
        }
    }

    @Override
    public List<TableIdentifier> findDDLChanged() {
        List<TableIdentifier> blockChangedIdentifiers = new ArrayList<>();

        try {
            executeQuery(blockChangedIdentifiers, findDDLChangeStatement);
        } catch (SQLException e) {
            // TODO
            log.error("Can not find ddl-changed.", e);
        }

        return blockChangedIdentifiers;
    }

    @Override
    public List<TableIdentifier> findDDLUnchanged() {
        List<TableIdentifier> ddlUnchanged = new ArrayList<>();
        try {
            executeQuery(ddlUnchanged, findDDLUnchangedStatement);
        } catch (SQLException e) {
            // TODO
            log.error("Can not find ddl-unchanged.", e);
        }
        return ddlUnchanged;
    }

    private void executeQuery(List<TableIdentifier> tableIdentifiers, PreparedStatement statement)
            throws SQLException {
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            String databaseName = resultSet.getString(DATABASE_NAME_KEY);
            String schemaName = resultSet.getString(SCHEMA_NAME_KEY);
            String tableName = resultSet.getString(TABLE_NAME_KEY);

            tableIdentifiers.add(new TableIdentifier(databaseName, schemaName, tableName));
        }
    }

    @Override
    public boolean insertDDLChange(DdlRowData data) {
        try {
            TableIdentifier tableIdentifier = data.getTableIdentifier();

            insertDDLChangeStatement.setString(1, tableIdentifier.getDataBase());
            insertDDLChangeStatement.setString(2, tableIdentifier.getSchema());
            insertDDLChangeStatement.setString(3, tableIdentifier.getTable());
            insertDDLChangeStatement.setString(4, data.getType().name());
            insertDDLChangeStatement.setString(5, data.getLsn());
            insertDDLChangeStatement.setInt(6, data.getLsnSequence());
            insertDDLChangeStatement.setString(7, data.getSql());

            if (data instanceof DdlRowDataConvented) {
                if (((DdlRowDataConvented) data).conventSuccessful()) {
                    insertDDLChangeStatement.setString(
                            8, ((DdlRowDataConvented) data).getConventInfo());
                    insertDDLChangeStatement.setInt(9, 0);
                    insertDDLChangeStatement.setString(10, null);
                } else {
                    insertDDLChangeStatement.setString(8, null);
                    insertDDLChangeStatement.setInt(9, -1);
                    insertDDLChangeStatement.setString(
                            10, ((DdlRowDataConvented) data).getConventInfo());
                }
            } else {
                insertDDLChangeStatement.setString(8, data.getSql());
                insertDDLChangeStatement.setInt(9, 0);
                insertDDLChangeStatement.setString(10, null);
            }

            insertDDLChangeStatement.execute();

            if (log.isDebugEnabled()) {
                String messageTemplate = "Insert ddl change succeed. Data = %s";
                log.debug(String.format(messageTemplate, data));
            }
            return true;
        } catch (Throwable e) {
            // TODO 异常优化
            log.warn("Insert DDL Data failed. ", e);
            throw new ChunJunRuntimeException("Insert DDL Data failed. ", e);
        }
    }

    @Override
    public void updateDDLChange(
            TableIdentifier tableIdentifier,
            String lsn,
            int lsnSequence,
            int status,
            String errorInfo) {
        try {
            if (tableIdentifier.getDataBase() != null) {

                updateDDLChangeStatement.setInt(1, status);
                updateDDLChangeStatement.setString(2, errorInfo);
                updateDDLChangeStatement.setString(3, tableIdentifier.getDataBase());
                updateDDLChangeStatement.setString(4, tableIdentifier.getSchema());
                updateDDLChangeStatement.setString(5, tableIdentifier.getTable());
                updateDDLChangeStatement.setString(6, lsn);
                updateDDLChangeStatement.setInt(7, lsnSequence);

                updateDDLChangeStatement.executeUpdate();
            } else {
                updateDDLChangeDataBaseNullAbleStatement.setInt(1, status);
                updateDDLChangeDataBaseNullAbleStatement.setString(2, errorInfo);
                updateDDLChangeDataBaseNullAbleStatement.setString(3, tableIdentifier.getSchema());
                updateDDLChangeDataBaseNullAbleStatement.setString(4, tableIdentifier.getTable());
                updateDDLChangeDataBaseNullAbleStatement.setString(5, lsn);
                updateDDLChangeDataBaseNullAbleStatement.setInt(6, lsnSequence);
                updateDDLChangeDataBaseNullAbleStatement.executeUpdate();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteChangedDDL(TableIdentifier tableIdentifier) {
        try {

            if (null != tableIdentifier.getDataBase()) {
                deleteChangedDDLStatement.setString(1, tableIdentifier.getDataBase());
                deleteChangedDDLStatement.setString(2, tableIdentifier.getSchema());
                deleteChangedDDLStatement.setString(3, tableIdentifier.getTable());
                deleteChangedDDLStatement.executeUpdate();
            } else {
                deleteChangedDDLStatementDataBaseNullAbleStatement.setString(
                        1, tableIdentifier.getSchema());
                deleteChangedDDLStatementDataBaseNullAbleStatement.setString(
                        2, tableIdentifier.getTable());
                deleteChangedDDLStatementDataBaseNullAbleStatement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new ChunJunRuntimeException(
                    "Delete changed ddl failed. TableIdentifier: " + tableIdentifier, e);
        }
    }
}
