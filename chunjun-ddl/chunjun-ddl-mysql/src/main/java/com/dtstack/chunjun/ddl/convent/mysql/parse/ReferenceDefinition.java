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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class ReferenceDefinition extends SqlCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("FOREIGN KEY DEFINITION", SqlKind.OTHER);

    public SqlIdentifier tableName;

    public SqlNodeList keyPartList;

    public SqlLiteral match;

    public SqlLiteral referenceSituation;
    public SqlLiteral referenceOption;

    public ReferenceDefinition(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList keyPartList,
            SqlLiteral match,
            SqlLiteral referenceSituation,
            SqlLiteral strategy) {
        super(pos);
        this.tableName = tableName;
        this.keyPartList = keyPartList;
        this.match = match;
        this.referenceSituation = referenceSituation;
        this.referenceOption = strategy;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

        writer.keyword("REFERENCES");
        tableName.unparse(writer, leftPrec, rightPrec);

        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode keyPart : keyPartList) {
            printIndent(writer);
            keyPart.unparse(writer, leftPrec, rightPrec);
        }

        writer.newlineAndIndent();
        writer.endList(frame);

        if (match != null) {
            match.unparse(writer, leftPrec, rightPrec);
        }

        if (referenceSituation != null) {
            referenceSituation.unparse(writer, leftPrec, rightPrec);
            referenceOption.unparse(writer, leftPrec, rightPrec);
        }
    }

    protected void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }
}
