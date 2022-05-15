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
package com.dtstack.chunjun.connector.jdbc.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * base on row data info to build preparedStatement. row data info include rowkind(which is to set
 * which sql kind to use )
 *
 * <p>Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-12-20
 */
public class DynamicPreparedStmt {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPreparedStmt.class);

    protected List<String> columnNameList = new ArrayList<>();

    protected List<String> columnTypeList = new ArrayList<>();

    protected transient FieldNamedPreparedStatement fieldNamedPreparedStatement;
    protected JdbcConf jdbcConf;
    private boolean writeExtInfo;
    private JdbcDialect jdbcDialect;
    private AbstractRowConverter<?, ?, ?, ?> rowConverter;

    public static DynamicPreparedStmt buildStmt(
            Map<String, Integer> header,
            Set<String> extHeader,
            String schemaName,
            String tableName,
            RowKind rowKind,
            Connection connection,
            JdbcDialect jdbcDialect,
            boolean writeExtInfo)
            throws SQLException {
        DynamicPreparedStmt dynamicPreparedStmt = new DynamicPreparedStmt();

        dynamicPreparedStmt.writeExtInfo = writeExtInfo;
        dynamicPreparedStmt.jdbcDialect = jdbcDialect;
        dynamicPreparedStmt.getColumnNameList(header, extHeader);
        dynamicPreparedStmt.getColumnMeta(schemaName, tableName, connection);
        dynamicPreparedStmt.buildRowConvert();

        String sql = dynamicPreparedStmt.prepareTemplates(rowKind, schemaName, tableName);
        String[] fieldNames = new String[dynamicPreparedStmt.columnNameList.size()];
        dynamicPreparedStmt.columnNameList.toArray(fieldNames);
        dynamicPreparedStmt.fieldNamedPreparedStatement =
                FieldNamedPreparedStatementImpl.prepareStatement(connection, sql, fieldNames);
        return dynamicPreparedStmt;
    }

    public static DynamicPreparedStmt buildStmt(
            String schemaName,
            String tableName,
            RowKind rowKind,
            Connection connection,
            JdbcDialect jdbcDialect,
            List<FieldConf> fieldConfList,
            AbstractRowConverter<?, ?, ?, ?> rowConverter)
            throws SQLException {
        DynamicPreparedStmt dynamicPreparedStmt = new DynamicPreparedStmt();
        dynamicPreparedStmt.jdbcDialect = jdbcDialect;
        dynamicPreparedStmt.rowConverter = rowConverter;
        String[] fieldNames = new String[fieldConfList.size()];
        for (int i = 0; i < fieldConfList.size(); i++) {
            FieldConf fieldConf = fieldConfList.get(i);
            fieldNames[i] = fieldConf.getName();
            dynamicPreparedStmt.columnNameList.add(fieldConf.getName());
            dynamicPreparedStmt.columnTypeList.add(fieldConf.getType());
        }
        String sql = dynamicPreparedStmt.prepareTemplates(rowKind, schemaName, tableName);
        dynamicPreparedStmt.fieldNamedPreparedStatement =
                FieldNamedPreparedStatementImpl.prepareStatement(connection, sql, fieldNames);
        return dynamicPreparedStmt;
    }

    public static DynamicPreparedStmt buildStmt(
            JdbcDialect jdbcDialect,
            List<FieldConf> fieldConfList,
            AbstractRowConverter<?, ?, ?, ?> rowConverter,
            FieldNamedPreparedStatement fieldNamedPreparedStatement) {
        DynamicPreparedStmt dynamicPreparedStmt = new DynamicPreparedStmt();
        dynamicPreparedStmt.jdbcDialect = jdbcDialect;
        dynamicPreparedStmt.rowConverter = rowConverter;
        dynamicPreparedStmt.fieldNamedPreparedStatement = fieldNamedPreparedStatement;
        for (int i = 0; i < fieldConfList.size(); i++) {
            FieldConf fieldConf = fieldConfList.get(i);
            dynamicPreparedStmt.columnNameList.add(fieldConf.getName());
            dynamicPreparedStmt.columnTypeList.add(fieldConf.getType());
        }
        return dynamicPreparedStmt;
    }

    protected String prepareTemplates(RowKind rowKind, String schemaName, String tableName) {
        String singleSql = null;
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                singleSql =
                        jdbcDialect.getInsertIntoStatement(
                                schemaName, tableName, columnNameList.toArray(new String[0]));
                break;
            case DELETE:
            case UPDATE_BEFORE:
                String[] columnNames = new String[columnNameList.size()];
                columnNameList.toArray(columnNames);
                singleSql = jdbcDialect.getDeleteStatement(schemaName, tableName, columnNames);
                break;
            default:
                // TODO 异常如何处理
                LOG.warn("not support RowKind: {}", rowKind);
        }

        return singleSql;
    }

    public void getColumnNameList(Map<String, Integer> header, Set<String> extHeader) {
        if (writeExtInfo) {
            columnNameList.addAll(header.keySet());
        } else {
            header.keySet().stream()
                    .filter(fieldName -> !extHeader.contains(fieldName))
                    .forEach(fieldName -> columnNameList.add(fieldName));
        }
    }

    public void buildRowConvert() {
        RowType rowType =
                TableUtil.createRowType(
                        columnNameList, columnTypeList, jdbcDialect.getRawTypeConverter());
        rowConverter = jdbcDialect.getColumnConverter(rowType, jdbcConf);
    }

    public void getColumnMeta(String schema, String table, Connection dbConn) {
        Pair<List<String>, List<String>> listListPair =
                JdbcUtil.getTableMetaData(null, schema, table, dbConn);
        List<String> nameList = listListPair.getLeft();
        List<String> typeList = listListPair.getRight();
        for (String columnName : columnNameList) {
            int index = nameList.indexOf(columnName);
            columnTypeList.add(typeList.get(index));
        }
    }

    public void close() throws SQLException {
        fieldNamedPreparedStatement.close();
    }

    public FieldNamedPreparedStatement getFieldNamedPreparedStatement() {
        return fieldNamedPreparedStatement;
    }

    public void setFieldNamedPreparedStatement(
            FieldNamedPreparedStatement fieldNamedPreparedStatement) {
        this.fieldNamedPreparedStatement = fieldNamedPreparedStatement;
    }

    public AbstractRowConverter getRowConverter() {
        return rowConverter;
    }

    public void setRowConverter(AbstractRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }
}
