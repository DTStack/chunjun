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

package com.dtstack.chunjun.ddl.convert.oracle.parse;

import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.ddl.convert.oracle.OracleTypeConvert;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;
import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.getSqlString;

public class SqlAddColumn extends SqlAlterTable {

    private static final OracleTypeConvert ORACLE_TYPE_CONVERT = new OracleTypeConvert();
    private final SqlNodeList columnList;
    private final SqlNodeList inLineConstraintList;

    public SqlAddColumn(
            SqlParserPos pos,
            SqlIdentifier tableIdentifier,
            SqlNodeList columnList,
            SqlNodeList constraintList) {
        super(pos, tableIdentifier);
        this.columnList = columnList;
        this.inLineConstraintList = constraintList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

        List<SqlGeneralColumn> generalColumnList = getGeneralColumnList();
        List<SqlConstraint> constraintList = getConstraintList();

        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ADD");

        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.OTHER, "(", ")");
        for (SqlGeneralColumn column : generalColumnList) {
            writer.sep(",", false);
            writer.newlineAndIndent();
            writer.print("  ");
            column.unparse(writer, leftPrec, rightPrec);
            for (SqlConstraint constraint : constraintList) {
                if (constraint.getColumns().get(0).equals(column.getName())) {
                    constraint.unparse(writer, leftPrec, rightPrec, true);
                }
            }
        }
        writer.newlineAndIndent();
        writer.endList(frame);
    }

    @Override
    public List<DdlOperator> parseToChunjunOperator() {
        List<SqlConstraint> constraintList = getConstraintList();

        List<DdlOperator> res = new ArrayList<>();
        List<ColumnDefinition> columnDefinitionList = new ArrayList<>();
        TableIdentifier identifier = convertSqlIdentifierToTableIdentifier(tableIdentifier);
        for (SqlNode sqlNode : columnList) {
            SqlGeneralColumn sqlGeneralColumn = (SqlGeneralColumn) sqlNode;
            ColumnDefinition columnDefinition =
                    sqlGeneralColumn.parseToChunjunOperator(ORACLE_TYPE_CONVERT);
            for (SqlConstraint sqlConstraint : constraintList) {
                if (getSqlString(sqlConstraint.getColumns().get(0))
                                .equals(getSqlString(sqlGeneralColumn.getName()))
                        && sqlConstraint.isNotNull()) {
                    columnDefinition.setNullable(false);
                    constraintList.remove(sqlConstraint);
                    break;
                }
            }
            columnDefinitionList.add(columnDefinition);
        }
        res.add(
                new ColumnOperator.Builder()
                        .type(EventType.ADD_COLUMN)
                        .sql(this.toSqlString(OracleSqlDialect.DEFAULT).getSql())
                        .tableIdentifier(identifier)
                        .columns(columnDefinitionList)
                        .build());
        constraintList.forEach(
                sqlConstraint -> res.add(sqlConstraint.parseToChunjunOperator(identifier)));
        return res;
    }

    private List<SqlGeneralColumn> getGeneralColumnList() {
        List<SqlGeneralColumn> generalColumnList = new ArrayList<>(columnList.size());
        for (SqlNode sqlNode : columnList) {
            generalColumnList.add((SqlGeneralColumn) sqlNode);
        }
        return generalColumnList;
    }

    private List<SqlConstraint> getConstraintList() {
        List<SqlConstraint> constraintList = new ArrayList<>(inLineConstraintList.size());
        for (SqlNode sqlNode : inLineConstraintList) {
            constraintList.add((SqlConstraint) sqlNode);
        }
        return constraintList;
    }
}
