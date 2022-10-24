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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlProcedureCharacteristic extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("PROCEDURE CHARACTERISTIC", SqlKind.OTHER);

    SqlNode comment = null;
    SqlLiteral sqlType = null;
    SqlLiteral languageSql = null;
    SqlLiteral sqlSecurity = null;

    public SqlProcedureCharacteristic(
            SqlParserPos pos,
            SqlNode comment,
            SqlLiteral sqlType,
            SqlLiteral languageSql,
            SqlLiteral sqlSecurity) {
        super(pos);
        this.comment = comment;
        this.sqlType = sqlType;
        this.languageSql = languageSql;
        this.sqlSecurity = sqlSecurity;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(comment, sqlType, languageSql, sqlSecurity);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (comment != null) {
            writer.keyword("COMMENT");
            if (!SqlNodeUtil.unparseSqlCharStringLiteral(comment, writer, leftPrec, rightPrec)) {
                comment.unparse(writer, leftPrec, rightPrec);
            }
        }
        if (languageSql != null) {
            languageSql.unparse(writer, leftPrec, rightPrec);
        }

        if (sqlType != null) {
            sqlType.unparse(writer, leftPrec, rightPrec);
        }

        if (sqlSecurity != null) {
            writer.keyword("SQL SECURITY");
            sqlSecurity.unparse(writer, leftPrec, rightPrec);
        }
    }
}
