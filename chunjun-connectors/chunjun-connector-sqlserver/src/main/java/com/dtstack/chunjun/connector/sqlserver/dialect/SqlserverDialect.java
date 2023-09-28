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

package com.dtstack.chunjun.connector.sqlserver.dialect;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputSplit;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.jdbc.util.key.DateTypeUtil;
import com.dtstack.chunjun.connector.jdbc.util.key.KeyUtil;
import com.dtstack.chunjun.connector.jdbc.util.key.NumericTypeUtil;
import com.dtstack.chunjun.connector.jdbc.util.key.TimestampTypeUtil;
import com.dtstack.chunjun.connector.sqlserver.converter.SqlserverJtdsRawTypeMapper;
import com.dtstack.chunjun.connector.sqlserver.converter.SqlserverJtdsSyncConverter;
import com.dtstack.chunjun.connector.sqlserver.converter.SqlserverMicroSoftRawTypeMapper;
import com.dtstack.chunjun.connector.sqlserver.converter.SqlserverMicroSoftSqlConverter;
import com.dtstack.chunjun.connector.sqlserver.converter.SqlserverMicroSoftSyncConverter;
import com.dtstack.chunjun.connector.sqlserver.util.increment.SqlserverTimestampTypeUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@NoArgsConstructor
public class SqlserverDialect implements JdbcDialect {

    private static final long serialVersionUID = 6935413064547142101L;

    private static final String SET_IDENTITY_INSERT_ON_SQL =
            "IF OBJECTPROPERTY(OBJECT_ID('%s'),'TableHasIdentity')=1 BEGIN SET IDENTITY_INSERT %s ON  END";

    private static final String WITH_NO_LOCK = " with(nolock)";

    /** Whether to add with(nolock) after the sql statement, the default is false */
    private boolean withNoLock;

    private boolean useJtdsDriver;

    public SqlserverDialect(boolean withNoLock, boolean useJtdsDriver) {
        this.withNoLock = withNoLock;
        this.useJtdsDriver = useJtdsDriver;
    }

    @Override
    public String dialectName() {
        return "SqlServer";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:sqlserver") || url.startsWith("jdbc:jtds:sqlserver");
    }

    @Override
    public RawTypeMapper getRawTypeConverter() {
        if (useJtdsDriver) {
            return SqlserverJtdsRawTypeMapper::apply;
        }
        return SqlserverMicroSoftRawTypeMapper::apply;
    }

    @Override
    public Optional<String> defaultDriverName() {
        if (useJtdsDriver) {
            return Optional.of("net.sourceforge.jtds.jdbc.Driver");
        } else {
            return Optional.of("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        }
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getColumnConverter(RowType rowType) {
        return getColumnConverter(rowType, null);
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getColumnConverter(RowType rowType, CommonConfig commonConfig) {
        if (useJtdsDriver) {
            return new SqlserverJtdsSyncConverter(rowType, commonConfig);
        }
        return new SqlserverMicroSoftSyncConverter(rowType, commonConfig);
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getRowConverter(RowType rowType) {
        return new SqlserverMicroSoftSqlConverter(rowType);
    }

    @Override
    public String getSelectFromStatement(
            String schemaName,
            String tableName,
            String customSql,
            String[] selectFields,
            String where) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        StringBuilder sql = new StringBuilder(128);
        sql.append("SELECT ");
        if (StringUtils.isNotBlank(customSql)) {
            sql.append("* FROM (")
                    .append(customSql)
                    .append(") ")
                    .append(JdbcUtil.TEMPORARY_TABLE_NAME);
        } else {
            sql.append(selectExpressions)
                    .append(" FROM ")
                    .append(buildTableInfoWithSchema(schemaName, tableName));
        }

        if (isWithNoLock()) {
            sql.append(WITH_NO_LOCK);
        }

        sql.append(" WHERE ");
        if (StringUtils.isNotBlank(where)) {
            sql.append(where);
        } else {
            sql.append(" 1=1 ");
        }

        return sql.toString();
    }

    @Override
    public String getSplitModFilter(JdbcInputSplit split, String splitPkName) {
        return String.format(
                "%s %% %s = %s",
                quoteIdentifier(splitPkName), split.getTotalNumberOfSplits(), split.getMod());
    }

    @Override
    public boolean supportUpsert() {
        return true;
    }

    @Override
    public Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean allReplace) {
        if (uniqueKeyFields == null || uniqueKeyFields.length == 0) {
            return Optional.of(getInsertIntoStatement(schema, tableName, fieldNames));
        }

        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        String values =
                Arrays.stream(fieldNames)
                        .map(i -> "T2." + quoteIdentifier(i))
                        .collect(Collectors.joining(","));

        List<String> updateColumns = getUpdateColumns(fieldNames, uniqueKeyFields);
        if (CollectionUtils.isEmpty(updateColumns)) {
            return Optional.of(
                    " MERGE INTO "
                            + buildTableInfoWithSchema(schema, tableName)
                            + " T1 USING "
                            + "("
                            + makeValues(fieldNames)
                            + ") T2 ON ("
                            + getUpdateFilterSql(uniqueKeyFields)
                            + ") WHEN NOT MATCHED THEN "
                            + "INSERT ("
                            + columns
                            + ") VALUES ("
                            + values
                            + ");");
        } else {

            String updates =
                    Arrays.stream(updateColumns.toArray(new String[0]))
                            .map(i -> "T1." + quoteIdentifier(i) + "=" + "T2." + quoteIdentifier(i))
                            .collect(Collectors.joining(","));
            return Optional.of(
                    " MERGE INTO "
                            + buildTableInfoWithSchema(schema, tableName)
                            + " T1 USING "
                            + "("
                            + makeValues(fieldNames)
                            + ") T2 ON ("
                            + getUpdateFilterSql(uniqueKeyFields)
                            + ") WHEN MATCHED THEN UPDATE SET "
                            + updates
                            + " WHEN NOT MATCHED THEN "
                            + "INSERT ("
                            + columns
                            + ") VALUES ("
                            + values
                            + ");");
        }
    }

    /**
     * Get the fields that need to be updated
     *
     * @param fieldNames
     * @param uniqueKeyFields
     * @return
     */
    public List<String> getUpdateColumns(String[] fieldNames, String[] uniqueKeyFields) {
        Set<String> uni = new HashSet<>(Arrays.asList(uniqueKeyFields));
        List<String> updateColumns = new ArrayList<>();
        for (String col : fieldNames) {
            if (!uni.contains(col)) {
                updateColumns.add(col);
            }
        }
        return updateColumns;
    }

    public String getUpdateFilterSql(String[] uniqueKeyFields) {
        List<String> list = new ArrayList<>();
        for (String uniqueKeyField : uniqueKeyFields) {
            String str =
                    "T1."
                            + quoteIdentifier(uniqueKeyField)
                            + "=T2."
                            + quoteIdentifier(uniqueKeyField);
            list.add(str);
        }
        return StringUtils.join(list, " AND ");
    }

    public String makeValues(String[] fieldNames) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < fieldNames.length; ++i) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(":" + fieldNames[i] + " " + quoteIdentifier(fieldNames[i]));
        }

        return sb.toString();
    }

    /**
     * When inserting the specified value into the identity field in the sqlserver table, SET
     * IDENTITY_INSERT = ON is required
     *
     * @param schema
     * @param table
     * @return
     */
    public String getIdentityInsertOnSql(String schema, String table) {
        String str = StringUtils.isEmpty(schema) ? table : schema + "." + table;
        return String.format(
                SET_IDENTITY_INSERT_ON_SQL, str, buildTableInfoWithSchema(schema, table));
    }

    public boolean isWithNoLock() {
        return withNoLock;
    }

    @Override
    public KeyUtil<?, BigInteger> initKeyUtil(String incrementName, TypeConfig incrementType) {
        switch (ColumnType.getType(incrementType.getType())) {
            case TIMESTAMP:
                return new SqlserverTimestampTypeUtil();
            case DATE:
                return new DateTypeUtil();
            default:
                if (ColumnType.isNumberType(incrementType.getType())) {
                    return new NumericTypeUtil();
                } else if (ColumnType.isTimeType(incrementType.getType())) {
                    return new TimestampTypeUtil();
                } else {
                    throw new ChunJunRuntimeException(
                            String.format(
                                    "Unsupported columnType [%s], columnName [%s]",
                                    incrementType, incrementName));
                }
        }
    }

    public Function<Tuple3<String, Integer, Integer>, TypeConfig> typeBuilder() {
        return (typePsTuple -> {
            String typeName = typePsTuple.f0.replace("identity", "");
            if (typeName.endsWith("()")) {
                typeName = typeName.replace("()", "").trim();
            }
            TypeConfig typeConfig = TypeConfig.fromString(typeName);
            typeConfig.setPrecision(typePsTuple.f1);
            typeConfig.setScale(typePsTuple.f2);
            return typeConfig;
        });
    }
}
