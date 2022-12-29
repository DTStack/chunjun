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
import com.dtstack.chunjun.cdc.ddl.definition.ColumnDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnOperator;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DataBaseOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.IndexDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.IndexOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableAddColumn;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableAddConstraint;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableAddIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableAlterColumn;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableChangeColumn;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableComment;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableConstraint;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableDrop;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableOperator;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableRename;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCreateDataBase;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCreateIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCreateTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropDataBase;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlDropTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlGeneralColumn;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlRenameTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlRenameTableSingleton;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlTableConstraint;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlTruncateTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.AlterTableRenameTypeEnum;
import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.AlterTableTargetTypeEnum;
import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.MysqlTableProperty;
import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.SqlConstraintEnforcement;
import com.dtstack.chunjun.ddl.convent.mysql.util.SqlNodeParseUtil;
import com.dtstack.chunjun.ddl.parse.SqlTableOption;
import com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SqlNodeParseImpl implements SqlNodeParse {
    private final ColumnTypeConvert columnTypeConvert = new MysqlTypeConvert();

    @Override
    public TableOperator parse(SqlCreateTable sqlCreate) {
        ArrayList<ColumnDefinition> columnDefinitions = new ArrayList<>();
        for (SqlNode sqlNode : sqlCreate.columnList) {
            SqlGeneralColumn column = (SqlGeneralColumn) sqlNode;
            columnDefinitions.add(SqlNodeParseUtil.parseColumn(column, columnTypeConvert));
        }

        ArrayList<IndexDefinition> indexDefinitions = new ArrayList<>();
        for (SqlNode sqlNode : sqlCreate.indexList) {
            SqlIndex index = (SqlIndex) sqlNode;
            indexDefinitions.add(SqlNodeParseUtil.parseIndex(index));
        }

        ArrayList<ConstraintDefinition> constraintDefinitions = new ArrayList<>();
        for (SqlNode sqlNode : sqlCreate.constraintList) {
            SqlTableConstraint constraint = (SqlTableConstraint) sqlNode;
            constraintDefinitions.add(SqlNodeParseUtil.parseConstraint(constraint));
        }

        boolean simple = sqlCreate.name.isSimple();
        String database = simple ? null : sqlCreate.name.names.get(0);
        String tableName = simple ? sqlCreate.name.names.get(0) : sqlCreate.name.names.get(1);

        TableIdentifier likeTableIdentifier = null;
        if (sqlCreate.likeTable) {
            if (sqlCreate.originalName.names.size() == 1) {
                likeTableIdentifier =
                        new TableIdentifier(null, null, sqlCreate.originalName.names.get(0));
            } else if (sqlCreate.originalName.names.size() == 2) {
                likeTableIdentifier =
                        new TableIdentifier(
                                sqlCreate.originalName.names.get(0),
                                null,
                                sqlCreate.originalName.names.get(1));
            } else if (sqlCreate.originalName.names.size() == 3) {
                likeTableIdentifier =
                        new TableIdentifier(
                                sqlCreate.originalName.names.get(0),
                                sqlCreate.originalName.names.get(1),
                                sqlCreate.originalName.names.get(2));
            }
        }
        String comment = null;
        if (sqlCreate.tableOptions != null
                && CollectionUtils.isNotEmpty(sqlCreate.tableOptions.getList())) {
            for (SqlNode sqlNode : sqlCreate.tableOptions.getList()) {
                if (sqlNode instanceof SqlTableOption) {
                    SqlTableOption sqlTableOption = (SqlTableOption) sqlNode;
                    if (sqlTableOption.getKey() instanceof SqlLiteral
                            && MysqlTableProperty.COMMENT.equals(
                                    ((SqlLiteral) sqlTableOption.getKey()).getValue())) {
                        comment =
                                sqlTableOption
                                        .getValue()
                                        .toSqlString(SqlNodeUtil.EMPTY_SQL_DIALECT)
                                        .getSql();
                    }
                }
            }
        }
        return new TableOperator(
                EventType.CREATE_TABLE,
                sqlCreate.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                new TableIdentifier(null, database, tableName),
                columnDefinitions,
                indexDefinitions,
                comment,
                constraintDefinitions,
                sqlCreate.isTemporary,
                sqlCreate.ifNotExists,
                sqlCreate.likeTable,
                likeTableIdentifier,
                null);
    }

    @Override
    public DataBaseOperator parse(SqlCreateDataBase sqlCreate) {
        String sql = sqlCreate.toSqlString(MysqlSqlDialect.DEFAULT).getSql();
        String name = sqlCreate.name.getSimple();
        return new DataBaseOperator(EventType.CREATE_DATABASE, sql, name);
    }

    @Override
    public TableOperator parse(SqlTruncateTable sqlTruncateTable) {
        boolean simple = sqlTruncateTable.sqlIdentifier.isSimple();
        String database = simple ? null : sqlTruncateTable.sqlIdentifier.names.get(0);
        String tableName =
                simple
                        ? sqlTruncateTable.sqlIdentifier.names.get(0)
                        : sqlTruncateTable.sqlIdentifier.names.get(1);

        return new TableOperator(
                EventType.TRUNCATE_TABLE,
                sqlTruncateTable.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                new TableIdentifier(null, database, tableName),
                null,
                null,
                null,
                null,
                false,
                false,
                false,
                null,
                null);
    }

    @Override
    public DataBaseOperator parse(SqlDropDataBase sqlDropDataBase) {
        String sql = sqlDropDataBase.toSqlString(MysqlSqlDialect.DEFAULT).getSql();
        String name = sqlDropDataBase.name.getSimple();
        return new DataBaseOperator(EventType.DROP_DATABASE, sql, name);
    }

    @Override
    public List<TableOperator> parse(SqlDropTable sqlDropTable) {
        List<TableIdentifier> collect =
                sqlDropTable.name.getList().stream()
                        .map(
                                i ->
                                        SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                                (SqlIdentifier) i))
                        .collect(Collectors.toList());

        return collect.stream()
                .map(
                        i ->
                                new TableOperator(
                                        EventType.DROP_TABLE,
                                        sqlDropTable.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                                        i,
                                        null,
                                        null,
                                        null,
                                        null,
                                        false,
                                        false,
                                        false,
                                        null,
                                        null))
                .collect(Collectors.toList());
    }

    @Override
    public List<TableOperator> parse(SqlRenameTable sqlRenameTable) {
        return sqlRenameTable.renameTables.getList().stream()
                .map(
                        i ->
                                new TableOperator(
                                        EventType.RENAME_TABLE,
                                        sqlRenameTable
                                                .toSqlString(MysqlSqlDialect.DEFAULT)
                                                .getSql(),
                                        SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                                ((SqlRenameTableSingleton) i).oldName),
                                        null,
                                        null,
                                        null,
                                        null,
                                        false,
                                        false,
                                        false,
                                        null,
                                        SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                                ((SqlRenameTableSingleton) i).newName)))
                .collect(Collectors.toList());
    }

    @Override
    public IndexOperator parse(SqlDropIndex sqlDropIndex) {
        TableIdentifier tableIdentifier =
                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(sqlDropIndex.tableName);
        return new IndexOperator(
                EventType.DROP_INDEX,
                sqlDropIndex.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                tableIdentifier,
                new IndexDefinition(null, sqlDropIndex.sqlIndex.name.getSimple(), null, null, null),
                null);
    }

    @Override
    public IndexOperator parse(SqlCreateIndex sqlCreateIndex) {

        TableIdentifier tableIdentifier =
                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(sqlCreateIndex.tableName);

        IndexDefinition indexDefinition = SqlNodeParseUtil.parseIndex(sqlCreateIndex.sqlIndex);

        return new IndexOperator(
                EventType.ADD_INDEX,
                sqlCreateIndex.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                tableIdentifier,
                indexDefinition,
                null);
    }

    @Override
    public List<DdlOperator> parse(SqlAlterTable sqlAlterTable) {
        ArrayList<DdlOperator> ddlOperators = new ArrayList<>();
        sqlAlterTable
                .alterTableOperatorList
                .getList()
                .forEach(
                        i -> {
                            SqlAlterTableOperator sqlAlterTableOperator = (SqlAlterTableOperator) i;
                            if (sqlAlterTableOperator instanceof SqlAlterTableAddColumn) {
                                ddlOperators.add(
                                        parse((SqlAlterTableAddColumn) sqlAlterTableOperator));
                                return;
                            } else if (sqlAlterTableOperator instanceof SqlAlterTableAddIndex) {
                                ddlOperators.add(
                                        parse((SqlAlterTableAddIndex) sqlAlterTableOperator));
                                return;
                            } else if (sqlAlterTableOperator
                                    instanceof SqlAlterTableAddConstraint) {
                                ddlOperators.add(
                                        parse((SqlAlterTableAddConstraint) sqlAlterTableOperator));
                                return;
                            } else if (sqlAlterTableOperator instanceof SqlAlterTableConstraint) {
                                ddlOperators.add(
                                        parse((SqlAlterTableConstraint) sqlAlterTableOperator));
                                return;
                            } else if (sqlAlterTableOperator instanceof SqlAlterTableComment) {
                                ddlOperators.add(
                                        parse((SqlAlterTableComment) sqlAlterTableOperator));
                                return;
                            } else if (sqlAlterTableOperator instanceof SqlAlterTableDrop) {
                                ddlOperators.add(parse((SqlAlterTableDrop) sqlAlterTableOperator));
                                return;
                            } else if (sqlAlterTableOperator instanceof SqlAlterTableAlterColumn) {
                                ddlOperators.add(
                                        parse((SqlAlterTableAlterColumn) sqlAlterTableOperator));
                                return;
                            } else if (sqlAlterTableOperator instanceof SqlAlterTableChangeColumn) {
                                ddlOperators.addAll(
                                        parse((SqlAlterTableChangeColumn) sqlAlterTableOperator));
                                return;
                            } else if (sqlAlterTableOperator instanceof SqlAlterTableRename) {
                                ddlOperators.add(
                                        parse((SqlAlterTableRename) sqlAlterTableOperator));
                                return;
                            }
                            throw new UnsupportedOperationException(
                                    "Unsupported parse SqlAlterTableOperator"
                                            + sqlAlterTableOperator.toSqlString(
                                                    MysqlSqlDialect.DEFAULT));
                        });

        return ddlOperators;
    }

    protected ColumnOperator parse(SqlAlterTableAddColumn sqlAlterTableAddColumn) {
        List<ColumnDefinition> columns =
                sqlAlterTableAddColumn.columns.getList().stream()
                        .map(
                                i ->
                                        SqlNodeParseUtil.parseColumn(
                                                (SqlGeneralColumn) i, columnTypeConvert))
                        .collect(Collectors.toList());

        return new ColumnOperator(
                EventType.ADD_COLUMN,
                sqlAlterTableAddColumn.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                        sqlAlterTableAddColumn.tableIdentifier),
                columns,
                false,
                false,
                null);
    }

    protected IndexOperator parse(SqlAlterTableAddIndex sqlAlterTableAddIndex) {
        TableIdentifier tableIdentifier =
                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                        sqlAlterTableAddIndex.tableIdentifier);
        IndexDefinition indexDefinition =
                SqlNodeParseUtil.parseIndex((SqlIndex) sqlAlterTableAddIndex.index);
        return new IndexOperator(
                EventType.ADD_INDEX,
                sqlAlterTableAddIndex.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                tableIdentifier,
                indexDefinition,
                null);
    }

    protected ConstraintOperator parse(SqlAlterTableAddConstraint sqlAlterTableAddConstraint) {
        TableIdentifier tableIdentifier =
                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                        sqlAlterTableAddConstraint.tableIdentifier);
        ConstraintDefinition constraintDefinition =
                SqlNodeParseUtil.parseConstraint(
                        (SqlTableConstraint) sqlAlterTableAddConstraint.constraint);
        return new ConstraintOperator(
                EventType.ADD_CONSTRAINT,
                sqlAlterTableAddConstraint.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                tableIdentifier,
                constraintDefinition);
    }

    protected ConstraintOperator parse(SqlAlterTableConstraint sqlAlterTableConstraint) {
        TableIdentifier tableIdentifier =
                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                        sqlAlterTableConstraint.tableIdentifier);

        ConstraintDefinition constraintDefinition =
                new ConstraintDefinition(
                        sqlAlterTableConstraint.symbol.getSimple(),
                        AlterTableTargetTypeEnum.PRIMARY_kEY.equals(
                                sqlAlterTableConstraint.altertType.getValue()),
                        false,
                        AlterTableTargetTypeEnum.CHECK.equals(
                                sqlAlterTableConstraint.altertType.getValue()),
                        null,
                        null,
                        null);

        boolean enforced =
                SqlConstraintEnforcement.ENFORCED.equals(
                        sqlAlterTableConstraint.enforced.getValue());
        return new ConstraintOperator(
                EventType.ALTER_CONSTRAINT_ENFORCED,
                sqlAlterTableConstraint.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                tableIdentifier,
                constraintDefinition,
                enforced,
                null);
    }

    protected DdlOperator parse(SqlAlterTableComment sqlAlterTableComment) {

        TableIdentifier tableIdentifier =
                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                        sqlAlterTableComment.tableIdentifier);

        return new TableOperator(
                EventType.ALTER_TABLE_COMMENT,
                sqlAlterTableComment.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                tableIdentifier,
                null,
                null,
                sqlAlterTableComment.comment.toSqlString(SqlNodeUtil.EMPTY_SQL_DIALECT).getSql(),
                null,
                false,
                false,
                false,
                null,
                null);
    }

    protected DdlOperator parse(SqlAlterTableDrop sqlAlterTableDrop) {
        return SqlNodeParseUtil.parseSqlAlterTableDrop(sqlAlterTableDrop);
    }

    protected ColumnOperator parse(SqlAlterTableAlterColumn sqlAlterTableAlterColumn) {
        return SqlNodeParseUtil.parseSqlAlterTableAlterColumn(sqlAlterTableAlterColumn);
    }

    protected List<ColumnOperator> parse(SqlAlterTableChangeColumn sqlAlterTableChangeColumn) {
        return SqlNodeParseUtil.parseSqlAlterTableChangeColumn(
                sqlAlterTableChangeColumn, columnTypeConvert);
    }

    protected DdlOperator parse(SqlAlterTableRename sqlAlterTableRename) {

        switch ((AlterTableRenameTypeEnum) sqlAlterTableRename.renameType.getValue()) {
            case KEY:
                return new ConstraintOperator(
                        EventType.RENAME_CONSTRAINT,
                        sqlAlterTableRename.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                        SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                sqlAlterTableRename.tableIdentifier),
                        new ConstraintDefinition(
                                sqlAlterTableRename.oldName.getSimple(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null),
                        null,
                        sqlAlterTableRename.newName.getSimple());
            case INDEX:
                return new IndexOperator(
                        EventType.RENAME_INDEX,
                        sqlAlterTableRename.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                        SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                sqlAlterTableRename.tableIdentifier),
                        new IndexDefinition(
                                null, sqlAlterTableRename.oldName.getSimple(), null, null, null),
                        sqlAlterTableRename.newName.getSimple());
            case COLUMN:
                return new ColumnOperator(
                        EventType.RENAME_COLUMN,
                        sqlAlterTableRename.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                        SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                sqlAlterTableRename.tableIdentifier),
                        Collections.singletonList(
                                new ColumnDefinition(
                                        sqlAlterTableRename.oldName.getSimple(),
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        null,
                        null,
                        sqlAlterTableRename.newName.getSimple());
            case TABLE:
                return new TableOperator(
                        EventType.RENAME_TABLE,
                        sqlAlterTableRename.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                        SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                sqlAlterTableRename.tableIdentifier),
                        null,
                        null,
                        null,
                        null,
                        false,
                        false,
                        false,
                        null,
                        SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                sqlAlterTableRename.newName));
            default:
                throw new UnsupportedOperationException(
                        "not support parse "
                                + sqlAlterTableRename.renameType.getValue()
                                + " of SqlAlterTableRename");
        }
    }
}
