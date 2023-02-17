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
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.ddl.convert.oracle.util.OracleSqlNodeParseUtil;
import com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;
import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.getSqlString;

public class SqlDropConstraint extends SqlAlterTable {

    private final SqlLiteral constraintSpec;
    private final SqlIdentifier constraintName;
    private final SqlNodeList columnNameList;

    public SqlDropConstraint(
            SqlParserPos pos,
            SqlIdentifier tableIdentifier,
            SqlLiteral constraintSpec,
            SqlIdentifier constraintName,
            SqlNodeList columnNameList) {
        super(pos, tableIdentifier);
        this.constraintSpec = constraintSpec;
        this.constraintName = constraintName;
        this.columnNameList = columnNameList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("DROP");
        writer.keyword(constraintSpec.toValue());
        if (constraintName != null) {
            constraintName.unparse(writer, leftPrec, rightPrec);
        }
        if (columnNameList != null) {
            SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.OTHER, "(", ")");
            OracleSqlNodeParseUtil.unParseSqlNodeList(columnNameList, writer, leftPrec, rightPrec);
            writer.newlineAndIndent();
            writer.endList(frame);
        }
    }

    @Override
    public List<DdlOperator> parseToChunjunOperator() {
        boolean isPrimaryKey = constraintSpec.toValue().equals("PRIMARY KEY");
        String name = null;
        if (!isPrimaryKey) {
            name = getSqlString(constraintName, SqlNodeUtil.EMPTY_SQL_DIALECT);
        }

        ConstraintDefinition constraintDefinition =
                new ConstraintDefinition.Builder().name(name).isPrimary(isPrimaryKey).build();

        return Collections.singletonList(
                new ConstraintOperator.Builder()
                        .type(EventType.DROP_CONSTRAINT)
                        .sql(getSqlString(this, OracleSqlDialect.DEFAULT))
                        .tableIdentifier(convertSqlIdentifierToTableIdentifier(tableIdentifier))
                        .constraintDefinition(constraintDefinition)
                        .build());
    }
}
