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
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;

public class SqlTruncateTable extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("TRUNCATE TABLE", SqlKind.OTHER_DDL);

    public SqlIdentifier sqlIdentifier;

    public SqlTruncateTable(SqlParserPos pos, SqlIdentifier sqlIdentifier) {
        super(pos);
        this.sqlIdentifier = sqlIdentifier;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(sqlIdentifier);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("TRUNCATE");
        writer.keyword("TABLE");
        sqlIdentifier.unparse(writer, leftPrec, rightPrec);
    }

    public void setSqlIdentifier(SqlIdentifier sqlIdentifier) {
        this.sqlIdentifier = sqlIdentifier;
    }

    public TableOperator parseToChunjunOperator() {
        return new TableOperator.Builder()
                .type(EventType.TRUNCATE_TABLE)
                .sql(this.toSqlString(OracleSqlDialect.DEFAULT).getSql())
                .tableIdentifier(convertSqlIdentifierToTableIdentifier(sqlIdentifier))
                .build();
    }
}
