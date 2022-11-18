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

package com.dtstack.chunjun.ddl.convent.mysql;

import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.cdc.ddl.ColumnTypeConvert;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DataBaseOperator;
import com.dtstack.chunjun.cdc.ddl.definition.IndexDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.IndexOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;
import com.dtstack.chunjun.ddl.convent.mysql.parse.KeyPart;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableRename;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCreateDataBase;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCreateIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropDataBase;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlIndexOption;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlMysqlConstraintEnable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlRenameTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlRenameTableSingleton;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlTruncateTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.AlterTableRenameTypeEnum;
import com.dtstack.chunjun.ddl.convent.mysql.util.SqlNodeParseUtil;
import com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class OperatorConventImpl implements OperatorConvent, Serializable {

    private final ColumnTypeConvert columnTypeConvert = new MysqlTypeConvert();

    @Override
    public String conventOperateTable(TableOperator operator) {
        SqlDialect sqlDialect = getSqlDialect();
        if (operator.getType().equals(EventType.DROP_TABLE)) {
            SqlDropTable sqlDropTable =
                    new SqlDropTable(
                            SqlParserPos.ZERO,
                            false,
                            true,
                            new SqlNodeList(
                                    Collections.singletonList(
                                            new SqlIdentifier(
                                                    operator.getTableDefinition()
                                                            .getTableIdentifier()
                                                            .getDataBase(),
                                                    SqlParserPos.ZERO)),
                                    SqlParserPos.ZERO),
                            null);
            return getSql(sqlDropTable);
        }

        if (operator.getType().equals(EventType.RENAME_TABLE)) {
            TableIdentifier oldTableIdentifier = operator.getTableDefinition().getTableIdentifier();
            TableIdentifier newTableIdentifier = operator.getNewTableIdentifier();

            SqlRenameTable sqlRenameTable =
                    new SqlRenameTable(
                            SqlParserPos.ZERO,
                            new SqlNodeList(
                                    Collections.singletonList(
                                            new SqlRenameTableSingleton(
                                                    SqlParserPos.ZERO,
                                                    SqlNodeUtil
                                                            .convertTableIdentifierToSqlIdentifier(
                                                                    oldTableIdentifier),
                                                    SqlNodeUtil
                                                            .convertTableIdentifierToSqlIdentifier(
                                                                    newTableIdentifier))),
                                    SqlParserPos.ZERO));
            return getSql(sqlRenameTable);
        }

        if (operator.getType().equals(EventType.TRUNCATE_TABLE)) {
            return new SqlTruncateTable(
                            SqlParserPos.ZERO,
                            SqlNodeUtil.convertTableIdentifierToSqlIdentifier(
                                    operator.getTableDefinition().getTableIdentifier()))
                    .toSqlString(getSqlDialect())
                    .getSql();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (operator.getTableDefinition().isTemporary()) {
            sb.append("TEMPORARY ");
        }
        sb.append("TABLE ");
        if (operator.getTableDefinition().isIfNotExists()) {
            sb.append("IF NOT EXISTS ");
        }
        if (StringUtils.isNotBlank(
                operator.getTableDefinition().getTableIdentifier().getDataBase())) {
            sb.append(
                            sqlDialect.quoteIdentifier(
                                    operator.getTableDefinition()
                                            .getTableIdentifier()
                                            .getDataBase()))
                    .append(".");
        }
        sb.append(
                        sqlDialect.quoteIdentifier(
                                operator.getTableDefinition().getTableIdentifier().getTable()))
                .append(" ");
        if (operator.isLikeTable()) {
            sb.append("LIKE ");
            TableIdentifier likeTableIdentifier = operator.getLikeTableIdentifier();

            if (likeTableIdentifier.getDataBase() != null) {
                sb.append(sqlDialect.quoteIdentifier(likeTableIdentifier.getDataBase()))
                        .append(".");
            }

            if (likeTableIdentifier.getTable() != null) {
                sb.append(sqlDialect.quoteIdentifier(likeTableIdentifier.getTable())).append(" ");
            }
        } else {
            sb.append(
                            SqlNodeParseUtil.parseCreateDefinition(
                                    operator.getTableDefinition().getColumnList(),
                                    operator.getTableDefinition().getIndexList(),
                                    operator.getTableDefinition().getConstraintList(),
                                    sqlDialect,
                                    columnTypeConvert))
                    .append(" ");
        }
        return sb.toString();
    }

    @Override
    public String conventOperateColumn(ColumnOperator operator) {
        SqlIdentifier sqlIdentifier =
                SqlNodeUtil.convertTableIdentifierToSqlIdentifier(operator.getTableIdentifier());

        if (operator.getType().equals(EventType.ADD_COLUMN)) {
            List<String> columns =
                    operator.getColumns().stream()
                            .map(
                                    i ->
                                            SqlNodeParseUtil.parseColumnDefinitionToString(
                                                    i, getSqlDialect(), columnTypeConvert))
                            .collect(Collectors.toList());

            return "ALTER TABLE "
                    + sqlIdentifier.toSqlString(getSqlDialect())
                    + "ADD COLUMN ("
                    + String.join(",", columns)
                    + ")";
        } else if (operator.getType().equals(EventType.DROP_COLUMN)) {
            return "ALTER TABLE "
                    + sqlIdentifier.toSqlString(getSqlDialect())
                    + "DROP COLUMN "
                    + getSqlDialect().quoteIdentifier(operator.getColumns().get(0).getName());
        } else if (operator.getType().equals(EventType.RENAME_COLUMN)) {
            return "ALTER TABLE "
                    + sqlIdentifier.toSqlString(getSqlDialect())
                    + "RENAME COLUMN "
                    + getSqlDialect().quoteIdentifier(operator.getColumns().get(0).getName())
                    + " TO "
                    + getSqlDialect().quoteIdentifier(operator.getNewName());
        }
        // todo EventType.ALTER_COLUMN
        throw new RuntimeException("not support ColumnOperator: " + operator);
    }

    @Override
    public String conventOperateDataBase(DataBaseOperator operator) {
        switch (operator.getType()) {
            case CREATE_DATABASE:
                SqlCreateDataBase sqlCreateDataBase =
                        new SqlCreateDataBase(
                                SqlParserPos.ZERO,
                                true,
                                true,
                                new SqlIdentifier(operator.getName(), SqlParserPos.ZERO),
                                SqlNodeList.EMPTY);
                return getSql(sqlCreateDataBase);

            case DROP_DATABASE:
                SqlDropDataBase sqlDropDataBase =
                        new SqlDropDataBase(
                                SqlParserPos.ZERO,
                                true,
                                new SqlIdentifier(operator.getName(), SqlParserPos.ZERO));
                return getSql(sqlDropDataBase);

            default:
                throw new UnsupportedOperationException("not support type: " + operator.getType());
        }
    }

    @Override
    public String conventOperateIndex(IndexOperator operator) {
        String sql;
        switch (operator.getType()) {
            case DROP_INDEX:
                sql = getCreateIndexSql(operator);
                break;
            case CREATE_INDEX:
                sql = getDropIndexSql(operator);
                break;

            case RENAME_INDEX:
                sql = getRenameIndexSql(operator);
                break;
            default:
                throw new UnsupportedOperationException("not support type: " + operator.getType());
        }

        return sql;
    }

    private String getSql(SqlCall sqlCall) {
        return sqlCall.toSqlString(getSqlDialect()).getSql();
    }

    private SqlDialect getSqlDialect() {
        return MysqlSqlDialect.DEFAULT;
    }

    private String getDropIndexSql(IndexOperator operator) {
        SqlIdentifier sqlIdentifier =
                SqlNodeUtil.convertTableIdentifierToSqlIdentifier(operator.getTableIdentifier());

        SqlDropIndex sqlDropIndex =
                new SqlDropIndex(
                        SqlParserPos.ZERO,
                        new SqlIndex(
                                SqlParserPos.ZERO,
                                new SqlIdentifier(
                                        operator.getIndex().getIndexName(), SqlParserPos.ZERO),
                                null,
                                null,
                                null,
                                null),
                        sqlIdentifier);
        return getSql(sqlDropIndex);
    }

    private String getCreateIndexSql(IndexOperator operator) {
        SqlIdentifier sqlIdentifier =
                SqlNodeUtil.convertTableIdentifierToSqlIdentifier(operator.getTableIdentifier());

        SqlNodeList sqlNodes = null;
        if (CollectionUtils.isNotEmpty(operator.getIndex().getColumns())) {
            ArrayList<KeyPart> keyParts = new ArrayList<>();
            for (IndexDefinition.ColumnInfo column : operator.getIndex().getColumns()) {
                keyParts.add(
                        new KeyPart(
                                SqlParserPos.ZERO,
                                new SqlIdentifier(column.getName(), SqlParserPos.ZERO),
                                null,
                                null));
            }
            sqlNodes = new SqlNodeList(keyParts, SqlParserPos.ZERO);
        }

        SqlCreateIndex sqlDropIndex =
                new SqlCreateIndex(
                        SqlParserPos.ZERO,
                        new SqlIndex(
                                SqlParserPos.ZERO,
                                new SqlIdentifier(
                                        operator.getIndex().getIndexName(), SqlParserPos.ZERO),
                                operator.getIndex().getIndexType() != null
                                        ? SqlLiteral.createSymbol(
                                                operator.getIndex().getIndexType(),
                                                SqlParserPos.ZERO)
                                        : null,
                                null,
                                sqlNodes,
                                new SqlIndexOption(
                                        SqlParserPos.ZERO,
                                        null,
                                        null,
                                        null,
                                        StringUtils.isNotBlank(operator.getIndex().getComment())
                                                ? SqlLiteral.createCharString(
                                                        operator.getIndex().getComment(),
                                                        "UTF-8",
                                                        SqlParserPos.ZERO)
                                                : null,
                                        operator.getIndex().getVisiable() != null
                                                        && !operator.getIndex().getVisiable()
                                                ? SqlMysqlConstraintEnable.INVISIBLE.symbol(
                                                        SqlParserPos.ZERO)
                                                : null,
                                        null,
                                        null)),
                        sqlIdentifier);
        return getSql(sqlDropIndex);
    }

    private String getRenameIndexSql(IndexOperator operator) {
        SqlIdentifier sqlIdentifier =
                SqlNodeUtil.convertTableIdentifierToSqlIdentifier(operator.getTableIdentifier());

        SqlAlterTableRename sqlAlterTableRename =
                new SqlAlterTableRename(
                        SqlParserPos.ZERO,
                        sqlIdentifier,
                        new SqlIdentifier(operator.getIndex().getIndexName(), SqlParserPos.ZERO),
                        new SqlIdentifier(operator.getNewName(), SqlParserPos.ZERO),
                        AlterTableRenameTypeEnum.INDEX.symbol(SqlParserPos.ZERO));
        return getSql(sqlAlterTableRename);
    }
}
