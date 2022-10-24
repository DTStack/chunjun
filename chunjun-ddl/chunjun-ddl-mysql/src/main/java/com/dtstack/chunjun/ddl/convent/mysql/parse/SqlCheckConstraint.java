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

package com.dtstack.chunjun.ddl.convent.mysql.parse;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlCheckConstraint extends SqlTableConstraint {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CONSTRAINT CHECK", SqlKind.CHECK);

    public final SqlNode expression;
    public final SqlLiteral enforced;

    public SqlCheckConstraint(
            SqlParserPos pos, SqlIdentifier name, SqlNode expression, SqlLiteral enforced) {
        super(pos, name, null, null, null, null, null, null);
        this.expression = expression;
        this.enforced = enforced;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, expression);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (name != null) {
            writer.keyword("CONSTRAINT");
            name.unparse(writer, 0, 0);
        }
        writer.keyword("CHECK");
        if (writer.isAlwaysUseParentheses()) {
            expression.unparse(writer, 0, 0);
        } else {
            writer.keyword("(");
            expression.unparse(writer, 0, 0);
            writer.keyword(")");
        }

        if (enforced != null) {
            enforced.unparse(writer, 0, 0);
        }
    }
}
