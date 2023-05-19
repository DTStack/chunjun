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
package com.dtstack.chunjun.connector.jdbc.sink.wrapper;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.conf.TableIdentify;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.wrapper.buffer.InsertDeleteCompactionWrapper;
import com.dtstack.chunjun.connector.jdbc.sink.wrapper.buffer.NoKeyInsertDeleteCompactionWrapper;
import com.dtstack.chunjun.connector.jdbc.sink.wrapper.proxy.NoKeyDeleteWrapperProxy;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class StatementWrapperUtil {

    /**
     * only use for restoreMode
     *
     * @param connection jdbc conn
     * @return {@link NoKeyInsertDeleteCompactionWrapper} or {@link InsertDeleteCompactionWrapper}
     */
    public static JdbcBatchStatementWrapper<RowData> buildInsertDeleteExecutor(
            Connection connection,
            JdbcDialect jdbcDialect,
            String schema,
            String table,
            ColumnRowData columnRowData)
            throws SQLException {
        TableIdentify tableIdentify = jdbcDialect.getTableIdentify(schema, table);
        String catalog = tableIdentify.getCatalog();
        schema = tableIdentify.getSchema();
        table = tableIdentify.getTable();
        List<String> columnNameList = getSelectedNameColumnList(columnRowData);
        List<TypeConfig> columnTypeList =
                getColumnTypeFromMetadata(
                        connection, jdbcDialect, catalog, schema, table, columnNameList);
        Pair<List<String>, List<TypeConfig>> primaryKeyInfoPair =
                getSelectedPrimaryKeyList(
                        connection, catalog, schema, table, columnNameList, columnTypeList);

        if (CollectionUtils.isNotEmpty(primaryKeyInfoPair.getLeft())) {
            return buildInsertDeleteKeyedExecutor(
                    connection,
                    schema,
                    table,
                    jdbcDialect,
                    columnNameList,
                    columnTypeList,
                    primaryKeyInfoPair.getLeft(),
                    primaryKeyInfoPair.getRight());
        } else {
            return buildNoKeyInsertDeleteExecutor(
                    connection, schema, table, jdbcDialect, columnNameList, columnTypeList);
        }
    }

    public static NoKeyInsertDeleteCompactionWrapper buildNoKeyInsertDeleteExecutor(
            Connection connection,
            String schema,
            String table,
            JdbcDialect jdbcDialect,
            List<String> columnNameList,
            List<TypeConfig> columnTypeList)
            throws SQLException {
        SimpleStatementWrapper insertStatementExecutor =
                getInsertStatementExecutor(
                        connection, schema, table, jdbcDialect, columnNameList, columnTypeList);

        NoKeyDeleteWrapperProxy deleteNoKeyStatementExecutor =
                new NoKeyDeleteWrapperProxy(
                        connection, schema, table, jdbcDialect, columnNameList, columnTypeList);

        return new NoKeyInsertDeleteCompactionWrapper(
                insertStatementExecutor, deleteNoKeyStatementExecutor);
    }

    public static InsertDeleteCompactionWrapper buildInsertDeleteKeyedExecutor(
            Connection connection,
            String schema,
            String table,
            JdbcDialect jdbcDialect,
            List<String> columnNameList,
            List<TypeConfig> columnTypeList,
            List<String> primaryColumnNameList,
            List<TypeConfig> primaryColumnTypeList)
            throws SQLException {
        SimpleStatementWrapper insertStatementExecutor =
                getInsertStatementExecutor(
                        connection, schema, table, jdbcDialect, columnNameList, columnTypeList);

        String keyedDeleteStatement =
                jdbcDialect.getKeyedDeleteStatement(schema, table, primaryColumnNameList);
        FieldNamedPreparedStatement fieldNamedPreparedStatement =
                FieldNamedPreparedStatement.prepareStatement(
                        connection,
                        keyedDeleteStatement,
                        primaryColumnNameList.toArray(new String[0]));
        RowType keyRowType =
                TableUtil.createRowType(
                        primaryColumnNameList,
                        primaryColumnTypeList,
                        jdbcDialect.getRawTypeConverter());
        SimpleStatementWrapper deleteStatementExecutor =
                new SimpleStatementWrapper(
                        fieldNamedPreparedStatement, jdbcDialect.getColumnConverter(keyRowType));

        return new InsertDeleteCompactionWrapper(
                insertStatementExecutor,
                deleteStatementExecutor,
                JdbcUtil.getKeyExtractor(columnNameList, primaryColumnNameList, keyRowType, true));
    }

    public static SimpleStatementWrapper getInsertStatementExecutor(
            Connection connection,
            String schema,
            String table,
            JdbcDialect jdbcDialect,
            List<String> columnNameList,
            List<TypeConfig> columnTypeList)
            throws SQLException {
        String insertStatement =
                jdbcDialect.getInsertIntoStatement(
                        schema, table, columnNameList.toArray(new String[0]));
        FieldNamedPreparedStatement fieldNamedPreparedStatement =
                FieldNamedPreparedStatement.prepareStatement(
                        connection, insertStatement, columnNameList.toArray(new String[0]));
        RowType rowType =
                TableUtil.createRowType(
                        columnNameList, columnTypeList, jdbcDialect.getRawTypeConverter());
        return new SimpleStatementWrapper(
                fieldNamedPreparedStatement, jdbcDialect.getColumnConverter(rowType));
    }

    /** only use for restoreMode,get fieldType for selectedColumn */
    public static List<TypeConfig> getColumnTypeFromMetadata(
            Connection connection,
            JdbcDialect jdbcDialect,
            String catalog,
            String schema,
            String table,
            List<String> selectedColumnNameList) {
        try {
            Pair<List<String>, List<TypeConfig>> tableMetaData =
                    jdbcDialect.getTableMetaData(
                            connection, schema, table, 300, null, selectedColumnNameList);
            List<String> metaColumnNameList = tableMetaData.getLeft();
            List<TypeConfig> metaColumnTypeList = tableMetaData.getRight();
            if (selectedColumnNameList.size() != metaColumnNameList.size()) {
                throw new RuntimeException(
                        "The length of the field on the source end is inconsistent with that on the sink end, and the sink table fields is "
                                + GsonUtil.GSON.toJson(metaColumnNameList)
                                + "  and the source table field is "
                                + GsonUtil.GSON.toJson(selectedColumnNameList));
            }
            List<TypeConfig> columnTypeList = new ArrayList<>(selectedColumnNameList.size());
            for (String columnName : selectedColumnNameList) {
                int j = 0;
                for (; j < metaColumnNameList.size(); j++) {
                    if (metaColumnNameList.get(j).equals(columnName)) {
                        columnTypeList.add(metaColumnTypeList.get(j));
                        break;
                    }
                }
                if (j >= metaColumnNameList.size()) {
                    throw new RuntimeException(
                            "The field "
                                    + columnName
                                    + " on the source end is not found on the sink end, and the sink table fields is "
                                    + GsonUtil.GSON.toJson(metaColumnNameList)
                                    + "  and the source table field is "
                                    + GsonUtil.GSON.toJson(selectedColumnNameList));
                }
            }
            return columnTypeList;
        } catch (Exception e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "get metadata failed,catalog[%s],schema[%s],table[%s]",
                            catalog, schema, table),
                    e);
        }
    }

    public static Pair<List<String>, List<TypeConfig>> getSelectedPrimaryKeyList(
            Connection connection,
            String catalog,
            String schema,
            String table,
            List<String> selectedColumnNameList,
            List<TypeConfig> selectedColumnTypeList) {
        try {
            List<String> primaryColumnNameList = new ArrayList<>();
            List<TypeConfig> primaryColumnTypeList = new ArrayList<>();
            ResultSet resultSet = connection.getMetaData().getPrimaryKeys(catalog, schema, table);
            while (resultSet.next()) {
                String primaryColumnName = resultSet.getString("COLUMN_NAME");
                for (int i = 0; i < selectedColumnNameList.size(); i++) {
                    String columnName = selectedColumnNameList.get(i);
                    TypeConfig columnType = selectedColumnTypeList.get(i);
                    if (columnName.equals(primaryColumnName)) {
                        primaryColumnNameList.add(columnName);
                        primaryColumnTypeList.add(columnType);
                    }
                }
            }
            return Pair.of(primaryColumnNameList, primaryColumnTypeList);
        } catch (SQLException e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "get primary key metadata failed,catalog[%s],schema[%s],table[%s]",
                            catalog, schema, table),
                    e);
        }
    }

    public static List<String> getSelectedNameColumnList(ColumnRowData columnRowData) {
        return new ArrayList<>(columnRowData.getHeaderInfo().keySet());
    }

    /**
     * Returns true if the row kind is INSERT or UPDATE_AFTER, returns false if the row kind is
     * DELETE or UPDATE_BEFORE.
     */
    public static boolean changeFlag(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return true;
            case DELETE:
            case UPDATE_BEFORE:
                return false;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER,"
                                        + " DELETE, but get: %s.",
                                rowKind));
        }
    }
}
