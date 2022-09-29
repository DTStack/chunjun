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

import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.ddl.convent.mysql.parse.KeyPart;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableAddColumn;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableAlterColumn;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableChangeColumn;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableDrop;
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
import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.AlterTableTargetTypeEnum;
import com.dtstack.chunjun.ddl.convent.mysql.util.SqlNodeParseUtil;
import com.dtstack.chunjun.ddl.parse.type.SqlCustomTypeNameSpec;
import com.dtstack.chunjun.mapping.Casing;
import com.dtstack.chunjun.mapping.MappingRule;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlNodeReplaceVisitor implements SqlVisitor<Void>, Serializable {

    private final MappingRule nameMapping;
    private TableIdentifier currentTableIdentifier;

    public SqlNodeReplaceVisitor(MappingRule nameMapping) {
        this.nameMapping = nameMapping;
    }

    @Override
    public Void visit(SqlLiteral literal) {
        return null;
    }

    public void setCurrentTableIdentifier(TableIdentifier tableIdentifier) {
        this.currentTableIdentifier = tableIdentifier;
    }

    @Override
    public Void visit(SqlCall call) {
        if (call instanceof SqlCreateTable) {
            SqlCreateTable sqlCreateTable = (SqlCreateTable) call;
            sqlCreateTable.name =
                    SqlNodeParseUtil.replaceTableIdentifier(
                            getActualIdentifier(sqlCreateTable.name, currentTableIdentifier),
                            nameMapping);
            if (sqlCreateTable.originalName != null) {
                sqlCreateTable.originalName =
                        SqlNodeParseUtil.replaceTableIdentifier(
                                getActualIdentifier(
                                        sqlCreateTable.originalName, currentTableIdentifier),
                                nameMapping);
            }

            ArrayList<String> columnNames = new ArrayList<>();

            if (sqlCreateTable.columnList != null
                    && CollectionUtils.isNotEmpty(sqlCreateTable.columnList.getList())) {
                sqlCreateTable
                        .columnList
                        .getList()
                        .forEach(
                                i -> {
                                    SqlGeneralColumn column = (SqlGeneralColumn) i;
                                    String customTypeName =
                                            nameMapping.mapType(
                                                    column.type.getTypeName().getSimple());
                                    if (!customTypeName.equals(
                                            column.type.getTypeName().getSimple())) {
                                        column.type =
                                                new SqlDataTypeSpec(
                                                        new SqlCustomTypeNameSpec(
                                                                customTypeName, SqlParserPos.ZERO),
                                                        SqlParserPos.ZERO);
                                    }

                                    columnNames.add(column.name.toString());
                                    if (nameMapping.getCasing() != Casing.UNCHANGE) {
                                        column.name =
                                                new SqlIdentifier(
                                                        nameMapping.casingName(
                                                                column.name.toString()),
                                                        SqlParserPos.ZERO);
                                    }
                                });
            }

            if (sqlCreateTable.indexList != null
                    && CollectionUtils.isNotEmpty(sqlCreateTable.indexList.getList())) {
                sqlCreateTable.indexList.forEach(
                        i -> {
                            if (i instanceof SqlIndex) {
                                SqlIndex sqlIndex = (SqlIndex) i;
                                if (null != sqlIndex.keyPartList) {
                                    replaceKeyPart(sqlIndex.keyPartList, columnNames);
                                }
                            }
                        });
            }

            if (sqlCreateTable.constraintList != null
                    && CollectionUtils.isNotEmpty(sqlCreateTable.constraintList.getList())) {
                sqlCreateTable.constraintList.forEach(
                        i -> {
                            if (i instanceof SqlTableConstraint) {
                                SqlTableConstraint sqlTableConstraint = (SqlTableConstraint) i;
                                if (null != sqlTableConstraint.keyPartList) {
                                    replaceKeyPart(sqlTableConstraint.keyPartList, columnNames);
                                }
                            }
                        });
            }

        } else if (call instanceof SqlDropTable) {
            List<SqlIdentifier> collect =
                    ((SqlDropTable) call)
                            .name.getList().stream()
                                    .map(
                                            i ->
                                                    SqlNodeParseUtil.replaceTableIdentifier(
                                                            getActualIdentifier(
                                                                    (SqlIdentifier) i,
                                                                    currentTableIdentifier),
                                                            nameMapping))
                                    .collect(Collectors.toList());
            ((SqlDropTable) call).name =
                    new SqlNodeList(collect, ((SqlDropTable) call).name.getParserPosition());
        } else if (call instanceof SqlRenameTable) {
            List<SqlRenameTableSingleton> collect =
                    ((SqlRenameTable) call)
                            .renameTables.getList().stream()
                                    .map(
                                            i -> {
                                                SqlRenameTableSingleton sqlRenameTableSingleton =
                                                        (SqlRenameTableSingleton) i;
                                                SqlIdentifier oldName =
                                                        SqlNodeParseUtil.replaceTableIdentifier(
                                                                getActualIdentifier(
                                                                        sqlRenameTableSingleton
                                                                                .oldName,
                                                                        currentTableIdentifier),
                                                                nameMapping);
                                                SqlIdentifier newName =
                                                        SqlNodeParseUtil.replaceTableIdentifier(
                                                                getActualIdentifier(
                                                                        sqlRenameTableSingleton
                                                                                .newName,
                                                                        currentTableIdentifier),
                                                                nameMapping);
                                                return new SqlRenameTableSingleton(
                                                        sqlRenameTableSingleton.getParserPosition(),
                                                        oldName,
                                                        newName);
                                            })
                                    .collect(Collectors.toList());
            ((SqlRenameTable) call).renameTables =
                    new SqlNodeList(
                            collect, ((SqlRenameTable) call).renameTables.getParserPosition());

        } else if (call instanceof SqlAlterTable) {
            SqlAlterTable sqlAlterTable = (SqlAlterTable) call;
            sqlAlterTable.tableIdentifier =
                    SqlNodeParseUtil.replaceTableIdentifier(
                            getActualIdentifier(
                                    sqlAlterTable.tableIdentifier, currentTableIdentifier),
                            nameMapping);
            sqlAlterTable.alterTableOperatorList.getList().stream()
                    .forEach(
                            i -> {
                                if (i instanceof SqlAlterTableAddColumn) {
                                    ((SqlAlterTableAddColumn) i)
                                            .columns.forEach(
                                                    column -> {
                                                        if (column instanceof SqlGeneralColumn) {
                                                            String customType =
                                                                    nameMapping.mapType(
                                                                            ((SqlGeneralColumn)
                                                                                            column)
                                                                                    .type
                                                                                    .getTypeName()
                                                                                    .getSimple());

                                                            if (!customType.equals(
                                                                    ((SqlGeneralColumn) column)
                                                                            .type
                                                                            .getTypeName()
                                                                            .getSimple())) {
                                                                SqlCustomTypeNameSpec
                                                                        sqlCustomTypeNameSpec =
                                                                                new SqlCustomTypeNameSpec(
                                                                                        customType,
                                                                                        SqlParserPos
                                                                                                .ZERO);
                                                                ((SqlGeneralColumn) column).type =
                                                                        new SqlDataTypeSpec(
                                                                                sqlCustomTypeNameSpec,
                                                                                SqlParserPos.ZERO);
                                                            }

                                                            if (nameMapping.getCasing()
                                                                            != Casing.UNCHANGE
                                                                    && ((SqlGeneralColumn) column)
                                                                                    .name
                                                                            != null) {
                                                                ((SqlGeneralColumn) column).name =
                                                                        new SqlIdentifier(
                                                                                nameMapping
                                                                                        .casingName(
                                                                                                ((SqlGeneralColumn)
                                                                                                                column)
                                                                                                                .name
                                                                                                                .toString()),
                                                                                SqlParserPos.ZERO);
                                                            }
                                                        }
                                                    });

                                } else if (i instanceof SqlAlterTableChangeColumn) {
                                    SqlAlterTableChangeColumn sqlAlterTableChangeColumn =
                                            (SqlAlterTableChangeColumn) i;
                                    if (nameMapping.getCasing() != Casing.UNCHANGE
                                            && sqlAlterTableChangeColumn.oldColumnName != null) {
                                        sqlAlterTableChangeColumn.oldColumnName =
                                                new SqlIdentifier(
                                                        nameMapping.casingName(
                                                                sqlAlterTableChangeColumn
                                                                        .oldColumnName.toString()),
                                                        SqlParserPos.ZERO);
                                    }

                                    if (sqlAlterTableChangeColumn.column
                                            instanceof SqlGeneralColumn) {
                                        SqlGeneralColumn column =
                                                (SqlGeneralColumn) sqlAlterTableChangeColumn.column;
                                        String customType =
                                                nameMapping.mapType(
                                                        column.type.getTypeName().getSimple());

                                        if (!customType.equals(
                                                column.type.getTypeName().getSimple())) {
                                            SqlCustomTypeNameSpec sqlCustomTypeNameSpec =
                                                    new SqlCustomTypeNameSpec(
                                                            customType, SqlParserPos.ZERO);
                                            column.type =
                                                    new SqlDataTypeSpec(
                                                            sqlCustomTypeNameSpec,
                                                            SqlParserPos.ZERO);
                                        }

                                        if (nameMapping.getCasing() != Casing.UNCHANGE) {
                                            column.name =
                                                    new SqlIdentifier(
                                                            nameMapping.casingName(
                                                                    column.name.toString()),
                                                            SqlParserPos.ZERO);
                                        }
                                    }
                                } else if (i instanceof SqlAlterTableDrop) {
                                    SqlAlterTableDrop sqlAlterTableDrop = (SqlAlterTableDrop) i;

                                    if (AlterTableTargetTypeEnum.COLUMN.equals(
                                                    sqlAlterTableDrop.dropType.getValue())
                                            && nameMapping.getCasing() != Casing.UNCHANGE) {
                                        sqlAlterTableDrop.name =
                                                new SqlIdentifier(
                                                        nameMapping.casingName(
                                                                sqlAlterTableDrop.name.toString()),
                                                        SqlParserPos.ZERO);
                                    }
                                } else if (i instanceof SqlAlterTableAlterColumn) {
                                    SqlAlterTableAlterColumn sqlAlterTableAlterColumn =
                                            (SqlAlterTableAlterColumn) i;
                                    if (nameMapping.getCasing() != Casing.UNCHANGE) {
                                        sqlAlterTableAlterColumn.symbol =
                                                new SqlIdentifier(
                                                        nameMapping.casingName(
                                                                sqlAlterTableAlterColumn.symbol
                                                                        .toString()),
                                                        SqlParserPos.ZERO);
                                    }
                                }
                            });

        } else if (call instanceof SqlCreateDataBase) {
            ((SqlCreateDataBase) call).name =
                    SqlNodeParseUtil.replaceTableIdentifier(
                            ((SqlCreateDataBase) call).name, nameMapping);
        } else if (call instanceof SqlDropDataBase) {
            ((SqlDropDataBase) call).name =
                    SqlNodeParseUtil.replaceTableIdentifier(
                            ((SqlCreateDataBase) call).name, nameMapping);

        } else if (call instanceof SqlDropIndex) {
            ((SqlDropIndex) call).tableName =
                    SqlNodeParseUtil.replaceTableIdentifier(
                            getActualIdentifier(
                                    ((SqlDropIndex) call).tableName, currentTableIdentifier),
                            nameMapping);
        } else if (call instanceof SqlCreateIndex) {
            ((SqlCreateIndex) call).tableName =
                    SqlNodeParseUtil.replaceTableIdentifier(
                            getActualIdentifier(
                                    ((SqlCreateIndex) call).tableName, currentTableIdentifier),
                            nameMapping);

            replaceKeyPart(((SqlCreateIndex) call).sqlIndex.keyPartList, null);

        } else if (call instanceof SqlTruncateTable) {
            ((SqlTruncateTable) call).sqlIdentifier =
                    SqlNodeParseUtil.replaceTableIdentifier(
                            getActualIdentifier(
                                    ((SqlTruncateTable) call).sqlIdentifier,
                                    currentTableIdentifier),
                            nameMapping);
        }
        return null;
    }

    public SqlIdentifier getActualIdentifier(
            SqlIdentifier sqlIdentifier, TableIdentifier tableIdentifier) {
        if (sqlIdentifier.isSimple()) {
            return new SqlIdentifier(
                    Lists.newArrayList(tableIdentifier.getSchema(), tableIdentifier.getTable()),
                    sqlIdentifier.getParserPosition());
        }
        return sqlIdentifier;
    }

    @Override
    public Void visit(SqlNodeList nodeList) {
        return null;
    }

    @Override
    public Void visit(SqlIdentifier id) {
        return null;
    }

    @Override
    public Void visit(SqlDataTypeSpec type) {
        return null;
    }

    @Override
    public Void visit(SqlDynamicParam param) {
        return null;
    }

    @Override
    public Void visit(SqlIntervalQualifier intervalQualifier) {
        return null;
    }

    public void replaceKeyPart(SqlNodeList keyPartList, List<String> columnNames) {

        keyPartList.forEach(
                key -> {
                    if (key instanceof KeyPart) {
                        KeyPart keyPart = (KeyPart) key;
                        if (CollectionUtils.isEmpty(columnNames)
                                || columnNames.contains(keyPart.colName.toString())) {
                            if (nameMapping.getCasing() != Casing.UNCHANGE) {
                                keyPart.colName =
                                        new SqlIdentifier(
                                                nameMapping.casingName(keyPart.colName.toString()),
                                                SqlParserPos.ZERO);
                            }
                        }
                    }
                });
    }
}
