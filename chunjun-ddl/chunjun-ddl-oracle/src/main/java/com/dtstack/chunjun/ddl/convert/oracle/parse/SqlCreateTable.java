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

import com.dtstack.chunjun.annotation.NotNull;
import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.cdc.ddl.ColumnTypeConvert;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;
import com.dtstack.chunjun.ddl.convert.oracle.OracleTypeConvert;
import com.dtstack.chunjun.mapping.MappingRule;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.ArrayList;
import java.util.List;

import static com.dtstack.chunjun.ddl.convert.oracle.util.OracleSqlNodeParseUtil.unParseSqlNodeList;
import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;

public class SqlCreateTable extends SqlCreate {

    private static final ColumnTypeConvert ORACLE_TYPE_CONVERT = new OracleTypeConvert();
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNodeList constraintList;

    public SqlCreateTable(
            SqlParserPos pos,
            SqlIdentifier name,
            boolean isTemporary,
            SqlNodeList columnList,
            SqlNodeList constraintList) {
        super(OPERATOR, pos, false, false);
        this.name = name;
        this.columnList = columnList;
        this.constraintList = constraintList;
    }

    @Override
    public @NotNull List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, constraintList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("TABLE");

        name.unparse(writer, leftPrec, rightPrec);

        if (columnList.size() > 0 || constraintList.size() > 0) {
            SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.OTHER, "(", ")");

            unParseSqlNodeList(columnList, writer, leftPrec, rightPrec);
            unParseSqlNodeList(constraintList, writer, leftPrec, rightPrec);

            writer.newlineAndIndent();
            writer.endList(frame);
        }
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public SqlNodeList getConstraintList() {
        return constraintList;
    }

    public TableOperator parseToTableOperator() {
        EventType eventType = EventType.CREATE_TABLE;
        String sql = this.toSqlString(OracleSqlDialect.DEFAULT).getSql();
        List<SqlConstraint> sqlConstraintList = new ArrayList<>(constraintList.size());
        for (SqlNode sqlNode : constraintList) {
            sqlConstraintList.add((SqlConstraint) sqlNode);
        }

        List<ColumnDefinition> columnDefinitionList = new ArrayList<>(constraintList.size());
        for (SqlNode sqlNode : columnList) {
            SqlGeneralColumn sqlGeneralColumn = (SqlGeneralColumn) sqlNode;
            ColumnDefinition columnDefinition =
                    sqlGeneralColumn.parseToChunjunOperator(ORACLE_TYPE_CONVERT);
            for (SqlConstraint sqlConstraint : sqlConstraintList) {
                if (sqlGeneralColumn.getName().equals(sqlConstraint.getColumns().get(0))
                        && sqlConstraint.isNotNull()) {
                    columnDefinition.setNullable(false);
                    sqlConstraintList.remove(sqlConstraint);
                    break;
                }
            }
            columnDefinitionList.add(columnDefinition);
        }

        List<ConstraintDefinition> constraintDefinitionList =
                new ArrayList<>(sqlConstraintList.size());
        sqlConstraintList.forEach(
                sqlConstraint ->
                        constraintDefinitionList.add(sqlConstraint.parseToConstraintDefinition()));

        return new TableOperator.Builder()
                .type(eventType)
                .sql(sql)
                .tableIdentifier(convertSqlIdentifierToTableIdentifier(name))
                .columnList(columnDefinitionList)
                .constraintList(constraintDefinitionList)
                .build();
    }

    public void nameMapping(MappingRule nameMapping, TableIdentifier currentTableIdentifier) {}
}
