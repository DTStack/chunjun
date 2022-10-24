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
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlIndex extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE_INDEX", SqlKind.CREATE_INDEX);

    public final SqlIdentifier name;
    public final SqlLiteral specialType;
    public final SqlNode indexType;
    public final SqlNodeList keyPartList;
    public final SqlNode indexOption;
    public final SqlLiteral algorithmOption;
    public final SqlLiteral lockOption;

    public SqlIndex(
            SqlParserPos pos,
            SqlIdentifier name,
            SqlLiteral specialType,
            SqlNode indexType,
            SqlNodeList ketPartList,
            SqlNode indexOption) {
        super(pos);
        this.name = name;
        this.specialType = specialType;
        this.indexType = indexType;
        this.keyPartList = ketPartList;
        this.indexOption = indexOption;
        this.algorithmOption = null;
        this.lockOption = null;
    }

    public SqlIndex(
            SqlParserPos pos,
            SqlIdentifier name,
            SqlLiteral specialType,
            SqlNode indexType,
            SqlNodeList ketPartList,
            SqlNode indexOption,
            SqlLiteral algorithmOption,
            SqlLiteral lockOption) {
        super(pos);
        this.name = name;
        this.specialType = specialType;
        this.indexType = indexType;
        this.keyPartList = ketPartList;
        this.indexOption = indexOption;
        this.algorithmOption = algorithmOption;
        this.lockOption = lockOption;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, specialType, indexType, keyPartList, indexOption);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (getSpecialType() != null) {
            specialType.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("INDEX");
        if (name != null) {
            name.unparse(writer, leftPrec, rightPrec);
        }
        if (indexType != null) {
            indexType.unparse(writer, leftPrec, rightPrec);
        }
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode keyPart : keyPartList) {
            printIndent(writer);
            keyPart.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);

        if (indexOption != null) {
            indexOption.unparse(writer, leftPrec, rightPrec);
        }
    }

    protected void printIndent(SqlWriter writer) {
        writer.sep(",", false);
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlLiteral getSpecialType() {
        return specialType;
    }

    public SqlNode getIndexType() {
        return indexType;
    }

    public SqlNodeList getKetPartList() {
        return keyPartList;
    }

    public SqlNode getIndexOption() {
        return indexOption;
    }
}
