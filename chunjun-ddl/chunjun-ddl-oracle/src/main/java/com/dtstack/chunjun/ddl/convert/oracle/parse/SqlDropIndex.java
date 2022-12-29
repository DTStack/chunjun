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
import com.dtstack.chunjun.cdc.ddl.definition.IndexDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.IndexOperator;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.getSqlString;

public class SqlDropIndex extends SqlCall {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP_INDEX", SqlKind.DROP_INDEX);

    private final SqlIdentifier indexIdentifier;

    public SqlDropIndex(SqlParserPos pos, SqlIdentifier indexIdentifier) {
        super(pos);
        this.indexIdentifier = indexIdentifier;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP INDEX");
        indexIdentifier.unparse(writer, leftPrec, rightPrec);
    }

    @NotNull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @NotNull
    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public IndexOperator parseToChunjunOperator() {
        IndexDefinition indexDefinition =
                new IndexDefinition.Builder().indexName(getSqlString(indexIdentifier)).build();
        return new IndexOperator(
                EventType.DROP_INDEX,
                this.toSqlString(OracleSqlDialect.DEFAULT).getSql(),
                null,
                indexDefinition,
                null);
    }
}
