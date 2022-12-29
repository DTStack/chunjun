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

package com.dtstack.chunjun.ddl.convert.oracle;

import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.cdc.ddl.ColumnTypeConvert;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnOperator;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DataBaseOperator;
import com.dtstack.chunjun.cdc.ddl.definition.ForeignKeyDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.IndexDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.IndexOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;
import com.dtstack.chunjun.ddl.convert.oracle.parse.SqlDropTable;
import com.dtstack.chunjun.ddl.convert.oracle.parse.SqlRenameTable;
import com.dtstack.chunjun.ddl.convert.oracle.parse.SqlTruncateTable;
import com.dtstack.chunjun.ddl.convert.oracle.util.OracleSqlNodeParseUtil;
import com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class OperatorConvertImpl implements OperatorConvert, Serializable {
    private final ColumnTypeConvert columnTypeConvert = new OracleTypeConvert();

    @Override
    public String convertOperateDataBase(DataBaseOperator operator) {
        throw new UnsupportedOperationException("not support parse" + operator.getType());
    }

    @Override
    public List<String> convertOperateTable(TableOperator operator) {
        if (operator.getType().equals(EventType.DROP_TABLE)) {
            SqlDropTable sqlDropTable =
                    new SqlDropTable(
                            SqlParserPos.ZERO,
                            false,
                            SqlNodeUtil.convertTableIdentifierToSqlIdentifier(
                                    operator.getTableDefinition().getTableIdentifier()),
                            false);
            return Collections.singletonList(getSql(sqlDropTable));
        } else if (operator.getType().equals(EventType.RENAME_TABLE)) {
            TableIdentifier oldTableIdentifier =
                    new TableIdentifier(
                            null,
                            null,
                            operator.getTableDefinition().getTableIdentifier().getTable());
            TableIdentifier newTableIdentifier =
                    new TableIdentifier(null, null, operator.getNewTableIdentifier().getTable());

            SqlRenameTable sqlRenameTable =
                    new SqlRenameTable(
                            SqlParserPos.ZERO,
                            SqlNodeUtil.convertTableIdentifierToSqlIdentifier(oldTableIdentifier),
                            SqlNodeUtil.convertTableIdentifierToSqlIdentifier(newTableIdentifier));
            return Collections.singletonList(getSql(sqlRenameTable));
        } else if (operator.getType().equals(EventType.CREATE_TABLE)) {
            ArrayList<String> sqls = new ArrayList<>();
            TableDefinition tableDefinition = operator.getTableDefinition();
            StringBuilder createTableBuilder = new StringBuilder();
            SqlIdentifier sqlIdentifier =
                    SqlNodeUtil.convertTableIdentifierToSqlIdentifier(
                            tableDefinition.getTableIdentifier());
            String identifier = sqlIdentifier.toSqlString(getSqlDialect()).getSql();
            createTableBuilder.append("CREATE TABLE ").append(identifier).append(" ");
            createTableBuilder
                    .append(
                            OracleSqlNodeParseUtil.parseCreateDefinition(
                                    operator.getTableDefinition().getColumnList(),
                                    operator.getTableDefinition().getConstraintList(),
                                    getSqlDialect(),
                                    columnTypeConvert))
                    .append(" ");
            sqls.add(createTableBuilder.toString());

            if (CollectionUtils.isNotEmpty(tableDefinition.getIndexList())) {
                for (IndexDefinition indexDefinition : tableDefinition.getIndexList()) {
                    String s =
                            OracleSqlNodeParseUtil.parseCreateIndexDefinitionToString(
                                    indexDefinition, sqlIdentifier, getSqlDialect());
                    sqls.add(s);
                }
            }
            // comment
            if (StringUtils.isNotBlank(tableDefinition.getComment())) {
                sqls.add(
                        OracleSqlNodeParseUtil.parseComment(
                                tableDefinition.getComment(),
                                sqlIdentifier,
                                false,
                                getSqlDialect()));
            }
            if (CollectionUtils.isNotEmpty(tableDefinition.getColumnList())) {
                tableDefinition.getColumnList().stream()
                        .filter(i -> StringUtils.isNotBlank(i.getComment()))
                        .forEach(
                                i ->
                                        sqls.add(
                                                OracleSqlNodeParseUtil.parseComment(
                                                        i.getComment(),
                                                        sqlIdentifier.add(
                                                                sqlIdentifier.names.size(),
                                                                i.getName(),
                                                                SqlParserPos.ZERO),
                                                        true,
                                                        getSqlDialect())));
            }
            return sqls;
        } else if (operator.getType().equals(EventType.TRUNCATE_TABLE)) {
            return Collections.singletonList(
                    new SqlTruncateTable(
                                    SqlParserPos.ZERO,
                                    SqlNodeUtil.convertTableIdentifierToSqlIdentifier(
                                            operator.getTableDefinition().getTableIdentifier()))
                            .toSqlString(getSqlDialect())
                            .getSql());
        } else if (operator.getType().equals(EventType.ALTER_TABLE_COMMENT)) {
            return Collections.singletonList(
                    "COMMENT ON TABLE "
                            + SqlNodeUtil.convertTableIdentifierToSqlIdentifier(
                                            operator.getTableDefinition().getTableIdentifier())
                                    .toSqlString(getSqlDialect())
                            + " IS '"
                            + operator.getTableDefinition().getComment()
                            + "'");
        }

        throw new UnsupportedOperationException("not support parse operator" + operator);
    }

    @Override
    public List<String> convertOperateColumn(ColumnOperator operator) {
        ArrayList<String> sqls = new ArrayList<>();
        SqlIdentifier sqlIdentifier =
                SqlNodeUtil.convertTableIdentifierToSqlIdentifier(operator.getTableIdentifier());

        if (EventType.ADD_COLUMN.equals(operator.getType())) {
            operator.getColumns()
                    .forEach(
                            i -> {
                                sqls.add(
                                        "ALTER TABLE "
                                                + sqlIdentifier
                                                        .toSqlString(getSqlDialect())
                                                        .getSql()
                                                + " ADD "
                                                + OracleSqlNodeParseUtil
                                                        .parseColumnDefinitionToString(
                                                                i,
                                                                getSqlDialect(),
                                                                columnTypeConvert));
                                if (i.getComment() != null) {
                                    sqls.add(
                                            "COMMENT ON COLUMN "
                                                    + sqlIdentifier
                                                            .toSqlString(getSqlDialect())
                                                            .getSql()
                                                    + "."
                                                    + getSqlDialect().quoteIdentifier(i.getName())
                                                    + " IS '"
                                                    + i.getComment()
                                                    + "'");
                                }
                            });

            return sqls;
        } else if (EventType.DROP_COLUMN.equals(operator.getType())) {
            operator.getColumns()
                    .forEach(
                            i -> {
                                sqls.add(
                                        "ALTER TABLE "
                                                + sqlIdentifier
                                                        .toSqlString(getSqlDialect())
                                                        .getSql()
                                                + " DROP COLUMN  "
                                                + getSqlDialect().quoteIdentifier(i.getName()));
                            });
            return sqls;
        } else if (EventType.RENAME_COLUMN.equals(operator.getType())) {
            operator.getColumns()
                    .forEach(
                            i -> {
                                sqls.add(
                                        "ALTER TABLE "
                                                + sqlIdentifier
                                                        .toSqlString(getSqlDialect())
                                                        .getSql()
                                                + " RENAME COLUMN  "
                                                + getSqlDialect().quoteIdentifier(i.getName())
                                                + " TO "
                                                + getSqlDialect()
                                                        .quoteIdentifier(operator.getNewName()));
                            });
            return sqls;
        } else if (EventType.ALTER_COLUMN.equals(operator.getType())) {
            operator.getColumns()
                    .forEach(
                            i -> {
                                if (operator.isDropDefault()) {
                                    sqls.add(
                                            "ALTER TABLE "
                                                    + sqlIdentifier
                                                            .toSqlString(getSqlDialect())
                                                            .getSql()
                                                    + " MODIFY "
                                                    + getSqlDialect().quoteIdentifier(i.getName())
                                                    + " DEFAULT NULL");
                                } else if (operator.isSetDefault()) {
                                    StringBuilder sb = new StringBuilder();
                                    sb.append("ALTER TABLE ")
                                            .append(
                                                    sqlIdentifier
                                                            .toSqlString(getSqlDialect())
                                                            .getSql())
                                            .append(" MODIFY ")
                                            .append(getSqlDialect().quoteIdentifier(i.getName()));

                                    OracleSqlNodeParseUtil.appendDefaultOption(i, sb);
                                    sqls.add(sb.toString());
                                } else {
                                    sqls.add(
                                            "ALTER TABLE "
                                                    + sqlIdentifier
                                                            .toSqlString(getSqlDialect())
                                                            .getSql()
                                                    + " MODIFY  "
                                                    + OracleSqlNodeParseUtil
                                                            .parseColumnDefinitionToString(
                                                                    i,
                                                                    getSqlDialect(),
                                                                    columnTypeConvert));

                                    if (i.getComment() != null) {
                                        sqls.add(
                                                "COMMENT ON COLUMN "
                                                        + sqlIdentifier
                                                                .toSqlString(getSqlDialect())
                                                                .getSql()
                                                        + "."
                                                        + getSqlDialect()
                                                                .quoteIdentifier(i.getName())
                                                        + " IS '"
                                                        + i.getComment()
                                                        + "'");
                                    }
                                }
                            });
            return sqls;
        }

        throw new UnsupportedOperationException("not support parse operator" + operator);
    }

    @Override
    public String convertOperateIndex(IndexOperator operator) {
        SqlIdentifier sqlIdentifier =
                SqlNodeUtil.convertTableIdentifierToSqlIdentifier(operator.getTableIdentifier());
        String sql;
        switch (operator.getType()) {
            case DROP_INDEX:
                sql =
                        "DROP INDEX "
                                + getSqlDialect()
                                        .quoteIdentifier(operator.getIndex().getIndexName());
                break;
            case ADD_INDEX:
                sql =
                        OracleSqlNodeParseUtil.parseCreateIndexDefinitionToString(
                                operator.getIndex(), sqlIdentifier, getSqlDialect());
                break;
            case RENAME_INDEX:
                sql =
                        "ALTER INDEX "
                                + getSqlDialect()
                                        .quoteIdentifier(operator.getIndex().getIndexName())
                                + " RENAME TO "
                                + getSqlDialect().quoteIdentifier(operator.getNewName());
                break;
            default:
                throw new UnsupportedOperationException("not support type: " + operator.getType());
        }

        return sql;
    }

    @Override
    public String convertOperatorConstraint(ConstraintOperator operator) {
        ConstraintDefinition constraintDefinition = operator.getConstraintDefinition();
        SqlIdentifier sqlIdentifier =
                SqlNodeUtil.convertTableIdentifierToSqlIdentifier(operator.getTableIdentifier());
        if (EventType.DROP_CONSTRAINT.equals(operator.getType())) {
            if (constraintDefinition.isPrimary()) {
                return "ALTER TABLE "
                        + sqlIdentifier.toSqlString(getSqlDialect()).getSql()
                        + " DROP PRIMARY KEY";
            } else if (constraintDefinition.isUnique()
                    || constraintDefinition.isCheck()
                    || constraintDefinition instanceof ForeignKeyDefinition) {
                return "ALTER TABLE "
                        + sqlIdentifier.toSqlString(getSqlDialect()).getSql()
                        + " DROP CONSTRAINT "
                        + getSqlDialect().quoteIdentifier(constraintDefinition.getName());
            }
        } else if (EventType.ADD_CONSTRAINT.equals(operator.getType())) {
            if (constraintDefinition.isPrimary()) {
                return "ALTER TABLE "
                        + sqlIdentifier.toSqlString(getSqlDialect()).getSql()
                        + " ADD CONSTRAINT "
                        + (StringUtils.isNotBlank(constraintDefinition.getName())
                                ? getSqlDialect().quoteIdentifier(constraintDefinition.getName())
                                : OracleSqlNodeParseUtil.DEFAULT_PRIMARY_KKY_prefix
                                        + UUID.randomUUID()
                                                .toString()
                                                .trim()
                                                .replace("-", "")
                                                .substring(0, 12))
                        + " PRIMARY KEY "
                        + " ("
                        + String.join(",", constraintDefinition.getColumns())
                        + " )";
            } else if (constraintDefinition.isUnique()) {
                return "ALTER TABLE "
                        + sqlIdentifier.toSqlString(getSqlDialect()).getSql()
                        + " add constraint "
                        + getSqlDialect().quoteIdentifier(constraintDefinition.getName())
                        + " unique"
                        + " ("
                        + String.join(",", constraintDefinition.getColumns())
                        + " )";
            } else if (constraintDefinition.isCheck()) {
                return "ALTER TABLE "
                        + sqlIdentifier.toSqlString(getSqlDialect()).getSql()
                        + " add constraint "
                        + getSqlDialect().quoteIdentifier(constraintDefinition.getName())
                        + " check"
                        + " ("
                        + constraintDefinition.getCheck()
                        + " )";
            } else if (constraintDefinition instanceof ForeignKeyDefinition) {
                return "ALTER TABLE "
                        + sqlIdentifier.toSqlString(getSqlDialect()).getSql()
                        + " add "
                        + OracleSqlNodeParseUtil.parseConstraintDefinitionToString(
                                constraintDefinition, getSqlDialect());
            }

        } else if (EventType.RENAME_CONSTRAINT.equals(operator.getType())) {
            return "ALTER TABLE "
                    + sqlIdentifier.toSqlString(getSqlDialect()).getSql()
                    + " rename constraint "
                    + getSqlDialect().quoteIdentifier(constraintDefinition.getName())
                    + " to  "
                    + getSqlDialect().quoteIdentifier(operator.getNewName());

        } else if (EventType.ALTER_CONSTRAINT_ENFORCED.equals(operator.getType())) {
            if (operator.getEnforced() != null) {
                if (operator.getEnforced()) {
                    return "ALTER TABLE "
                            + sqlIdentifier.toSqlString(getSqlDialect()).getSql()
                            + " enable constraint "
                            + getSqlDialect().quoteIdentifier(constraintDefinition.getName());
                } else {
                    return "ALTER TABLE "
                            + sqlIdentifier.toSqlString(getSqlDialect()).getSql()
                            + " disable constraint "
                            + getSqlDialect().quoteIdentifier(constraintDefinition.getName());
                }
            }
        }
        throw new UnsupportedOperationException("not support parse" + operator.getType());
    }

    private String getSql(SqlCall sqlCall) {
        return sqlCall.toSqlString(getSqlDialect()).getSql();
    }

    private SqlDialect getSqlDialect() {
        return OracleSqlDialect.DEFAULT;
    }
}
