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

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlCreateIndex extends SqlCreate {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE INDEX", SqlKind.CREATE_INDEX);

    public SqlIdentifier tableName;
    public SqlIndex sqlIndex;

    public SqlCreateIndex(SqlParserPos pos, SqlIndex sqlIndex, SqlIdentifier tableName) {
        super(OPERATOR, pos, false, false);
        this.tableName = tableName;
        this.sqlIndex = sqlIndex;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableName, sqlIndex);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (sqlIndex.specialType != null) {
            sqlIndex.specialType.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("INDEX");

        sqlIndex.name.unparse(writer, leftPrec, rightPrec);
        if (sqlIndex.indexType != null) {
            sqlIndex.indexType.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("ON");

        tableName.unparse(writer, leftPrec, rightPrec);

        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode keyPart : sqlIndex.keyPartList) {
            printIndent(writer);
            keyPart.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);

        if (sqlIndex.indexOption != null) {
            sqlIndex.indexOption.unparse(writer, leftPrec, rightPrec);
        }

        if (sqlIndex.algorithmOption != null) {
            writer.keyword("ALGORITHM");
            writer.keyword("=");
            sqlIndex.algorithmOption.unparse(writer, leftPrec, rightPrec);
        }

        if (sqlIndex.lockOption != null) {
            writer.keyword("LOCK");
            writer.keyword("=");
            sqlIndex.lockOption.unparse(writer, leftPrec, rightPrec);
        }
    }

    protected void printIndent(SqlWriter writer) {
        writer.sep(",", false);
    }
}
