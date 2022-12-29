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
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public abstract class SqlAlterIndex extends SqlCall {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("ALTER_INDEX", SqlKind.ALTER_INDEX);

    protected final SqlIdentifier indexIdentifier;

    public SqlAlterIndex(SqlParserPos pos, SqlIdentifier indexIdentifier) {
        super(pos);
        this.indexIdentifier = indexIdentifier;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER INDEX");
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

    public abstract DdlOperator parseToChunjunOperator();
}
