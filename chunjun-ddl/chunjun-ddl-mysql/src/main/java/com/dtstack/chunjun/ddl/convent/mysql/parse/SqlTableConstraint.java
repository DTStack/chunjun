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

import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.SqlConstraintSpec;

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

public class SqlTableConstraint extends SqlCall {
    private static final SqlSpecialOperator UNIQUE =
            new SqlSpecialOperator("UNIQUE", SqlKind.UNIQUE);

    protected static final SqlSpecialOperator PRIMARY =
            new SqlSpecialOperator("PRIMARY KEY", SqlKind.PRIMARY_KEY);

    protected static final SqlSpecialOperator FOREIGN_KEY =
            new SqlSpecialOperator("FOREIGN KEY", SqlKind.FOREIGN_KEY);

    public SqlIdentifier name;
    public SqlLiteral uniqueSpec;
    public SqlIdentifier indexName;
    public SqlNodeList keyPartList;
    public SqlNode indexType;

    // PRIMARY KEY | UNIQUE OPTION
    public SqlNode indexOption;

    // FOREIGN KEY DEFINITION
    public SqlNode referenceDefinition;

    public SqlTableConstraint(
            SqlParserPos pos,
            SqlIdentifier name,
            SqlLiteral uniqueSpec,
            SqlIdentifier indexName,
            SqlNodeList keyPartList,
            SqlNode indexType,
            SqlNode referenceDefinition,
            SqlNode indexOption) {
        super(pos);
        this.name = name;
        this.uniqueSpec = uniqueSpec;
        this.indexName = indexName;
        this.keyPartList = keyPartList;
        this.indexType = indexType;
        this.indexOption = indexOption;
        this.referenceDefinition = referenceDefinition;
    }

    @Override
    public SqlOperator getOperator() {
        if (SqlConstraintSpec.UNIQUE.name().equalsIgnoreCase(uniqueSpec.toValue())
                | SqlConstraintSpec.UNIQUE_KEY.name().equalsIgnoreCase(uniqueSpec.toValue())
                | SqlConstraintSpec.UNIQUE_INDEX.name().equalsIgnoreCase(uniqueSpec.toValue())) {
            return UNIQUE;
        }
        if (SqlConstraintSpec.PRIMARY_KEY.name().equalsIgnoreCase(uniqueSpec.toValue())) {
            return PRIMARY;
        }
        if (SqlConstraintSpec.FOREIGN_KEY.name().equalsIgnoreCase(uniqueSpec.toValue())) {
            return FOREIGN_KEY;
        }
        throw new IllegalArgumentException("not contains " + uniqueSpec.toValue() + "operator");
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CONSTRAINT");
        if (name != null) {
            name.unparse(writer, leftPrec, rightPrec);
        }
        uniqueSpec.unparse(writer, leftPrec, rightPrec);

        if (indexName != null) {
            indexName.unparse(writer, leftPrec, rightPrec);
        }

        if (indexType != null) {
            indexType.unparse(writer, leftPrec, rightPrec);
        }

        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode keyPart : keyPartList) {
            printIndent(writer);
            keyPart.unparse(writer, leftPrec, rightPrec);
        }

        writer.newlineAndIndent();
        writer.endList(frame);

        if (indexOption != null) {
            indexOption.unparse(writer, leftPrec, rightPrec);
        }

        if (referenceDefinition != null) {
            referenceDefinition.unparse(writer, leftPrec, rightPrec);
        }
    }

    protected void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }
}
