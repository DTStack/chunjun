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
package com.dtstack.chunjun.connector.jdbc.sink.wrapper.proxy;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.wrapper.SimpleStatementWrapper;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.types.logical.RowType;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * base on row data info to build preparedStatement. row data info include rowkind(which is to set
 * which sql kind to use )
 */
public class DynamicSimpleDeleteWrapper extends SimpleStatementWrapper {

    protected List<String> columnNameList = new ArrayList<>();

    protected List<TypeConfig> columnTypeList = new ArrayList<>();
    protected List<String> nullColumnNameList = new ArrayList<>();

    protected JdbcConfig jdbcConfig;
    private JdbcDialect jdbcDialect;

    public DynamicSimpleDeleteWrapper(
            FieldNamedPreparedStatement statement, AbstractRowConverter converter) {
        super(statement, converter);
    }

    public static DynamicSimpleDeleteWrapper buildExecutor(
            List<String> columnNameList,
            List<TypeConfig> columnTypeList,
            List<String> nullColumnNameList,
            String schemaName,
            String tableName,
            Connection connection,
            JdbcDialect jdbcDialect)
            throws SQLException {

        DynamicSimpleDeleteWrapper dynamicPreparedStmt = new DynamicSimpleDeleteWrapper(null, null);

        dynamicPreparedStmt.jdbcDialect = jdbcDialect;
        dynamicPreparedStmt.columnNameList = columnNameList;
        dynamicPreparedStmt.columnTypeList = columnTypeList;
        dynamicPreparedStmt.nullColumnNameList = nullColumnNameList;
        dynamicPreparedStmt.buildRowConvert();

        // adapt to pg
        dynamicPreparedStmt.adaptToPostgresRowConverter(
                dynamicPreparedStmt.rowConverter, connection);

        String sql = dynamicPreparedStmt.prepareTemplates(schemaName, tableName);
        String[] fieldNames = new String[dynamicPreparedStmt.columnNameList.size()];
        dynamicPreparedStmt.columnNameList.toArray(fieldNames);

        String[] nullFieldNames = new String[dynamicPreparedStmt.nullColumnNameList.size()];
        dynamicPreparedStmt.nullColumnNameList.toArray(nullFieldNames);
        dynamicPreparedStmt.statement =
                FieldNamedPreparedStatementImpl.prepareStatement(
                        connection, sql, fieldNames, nullFieldNames);
        return dynamicPreparedStmt;
    }

    protected String prepareTemplates(String schemaName, String tableName) {

        String[] columnNames = new String[columnNameList.size()];
        columnNameList.toArray(columnNames);

        String[] nullColumnNames = new String[nullColumnNameList.size()];
        nullColumnNameList.toArray(nullColumnNames);

        return jdbcDialect.getDeleteStatement(schemaName, tableName, columnNames, nullColumnNames);
    }

    public void buildRowConvert() {
        RowType rowType =
                TableUtil.createRowType(
                        columnNameList, columnTypeList, jdbcDialect.getRawTypeConverter());
        rowConverter = jdbcDialect.getColumnConverter(rowType, jdbcConfig);
    }

    public void adaptToPostgresRowConverter(AbstractRowConverter rowConv, Connection conn) {
        try {
            if (rowConv.getClass().getSimpleName().equals("PostgresqlColumnConverter")) {
                Class pgConn = Class.forName("org.postgresql.core.BaseConnection");
                Method setConnection =
                        rowConv.getClass().getDeclaredMethod("setConnection", pgConn);
                setConnection.invoke(rowConv, conn);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
