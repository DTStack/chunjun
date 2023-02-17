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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;
import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.getSqlString;

public class SqlRenameConstraint extends SqlAlterTable {

    private SqlIdentifier oldName;
    private SqlIdentifier newName;

    public SqlRenameConstraint(
            SqlParserPos pos,
            SqlIdentifier tableIdentifier,
            SqlIdentifier oldName,
            SqlIdentifier newName) {
        super(pos, tableIdentifier);
        this.oldName = oldName;
        this.newName = newName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("RENAME CONSTRAINT");
        oldName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("TO");
        newName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public List<DdlOperator> parseToChunjunOperator() {
        ConstraintDefinition constraintDefinition =
                new ConstraintDefinition.Builder().name(getSqlString(oldName)).build();

        return Collections.singletonList(
                new ConstraintOperator.Builder()
                        .type(EventType.RENAME_CONSTRAINT)
                        .sql(this.toSqlString(OracleSqlDialect.DEFAULT).getSql())
                        .tableIdentifier(convertSqlIdentifierToTableIdentifier(tableIdentifier))
                        .constraintDefinition(constraintDefinition)
                        .newName(getSqlString(newName))
                        .build());
    }
}
