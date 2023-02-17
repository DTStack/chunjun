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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;

/** Parse tree for {@code DROP VIEW} statement. */
public class SqlDropTable extends SqlDrop {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP TABLE", SqlKind.DROP_VIEW);

    public SqlIdentifier name;
    private Boolean isCascadeConstraints;

    public SqlDropTable(
            SqlParserPos pos, boolean ifExists, SqlIdentifier name, Boolean isCascadeConstraints) {
        super(OPERATOR, pos, ifExists);
        this.name = name;
        this.isCascadeConstraints = isCascadeConstraints;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(this.name);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        writer.keyword("TABLE");
        name.unparse(writer, leftPrec, rightPrec);
        if (isCascadeConstraints != null && isCascadeConstraints) {
            writer.keyword(" CASCADE CONSTRAINTS ");
        }
    }

    public TableOperator parseToChunJunOperator() {
        return new TableOperator.Builder()
                .type(EventType.DROP_TABLE)
                .sql(this.toSqlString(OracleSqlDialect.DEFAULT).getSql())
                .tableIdentifier(convertSqlIdentifierToTableIdentifier(name))
                .build();
    }
}
