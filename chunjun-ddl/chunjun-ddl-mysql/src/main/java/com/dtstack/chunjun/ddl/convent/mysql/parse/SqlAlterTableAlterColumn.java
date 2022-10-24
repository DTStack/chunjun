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

import com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil;

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

public class SqlAlterTableAlterColumn extends SqlAlterTableOperator {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER TABLE ALTER COLUMN", SqlKind.ALTER_TABLE);

    public SqlIdentifier symbol;
    public SqlLiteral operator;
    public SqlNode value;

    public SqlAlterTableAlterColumn(
            SqlParserPos pos,
            SqlIdentifier tableIdentifier,
            SqlIdentifier symbol,
            SqlLiteral operator,
            SqlNode value) {
        super(pos, tableIdentifier);
        this.symbol = symbol;
        this.operator = operator;
        this.value = value;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableIdentifier, symbol, operator, value);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        //        writer.keyword("ALTER TABLE");
        //        tableIdentifier.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ALTER");
        writer.keyword("COLUMN");
        symbol.unparse(writer, leftPrec, rightPrec);
        operator.unparse(writer, leftPrec, rightPrec);
        if (value != null) {
            if (!SqlNodeUtil.unparseSqlCharStringLiteral(value, writer, leftPrec, rightPrec)) {
                value.unparse(writer, leftPrec, rightPrec);
            }
        }
    }
}
