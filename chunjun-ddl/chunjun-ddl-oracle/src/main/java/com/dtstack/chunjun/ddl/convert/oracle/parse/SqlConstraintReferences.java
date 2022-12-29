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
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ForeignKeyDefinition;
import com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil;

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

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;

public class SqlConstraintReferences extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("FOREIGN KEY DEFINITION", SqlKind.OTHER);

    private final SqlIdentifier referenceTable;

    private final SqlNodeList keyPartList;
    private final SqlLiteral referenceSituation;
    private final SqlLiteral referenceOption;

    public SqlConstraintReferences(
            SqlParserPos pos,
            SqlIdentifier referenceTable,
            SqlNodeList keyPartList,
            SqlLiteral referenceSituation,
            SqlLiteral referenceOption) {
        super(pos);
        this.referenceTable = referenceTable;
        this.keyPartList = keyPartList;
        this.referenceSituation = referenceSituation;
        this.referenceOption = referenceOption;
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

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("REFERENCES");
        referenceTable.unparse(writer, leftPrec, rightPrec);

        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode keyPart : keyPartList) {
            printIndent(writer);
            keyPart.unparse(writer, leftPrec, rightPrec);
        }

        writer.newlineAndIndent();
        writer.endList(frame);

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

    public ConstraintDefinition parseToForeignKeyDefinition(
            String constraintName, List<String> columnNameList) {
        List<String> referenceColumnNameList = SqlNodeUtil.parseNodeListString(keyPartList);
        ForeignKeyDefinition.Constraint onDeleteConstraint = parseConstraint();

        return new ForeignKeyDefinition.Builder()
                .name(constraintName)
                .columns(columnNameList)
                .referenceTable(convertSqlIdentifierToTableIdentifier(referenceTable))
                .referenceColumns(referenceColumnNameList)
                .onDelete(onDeleteConstraint)
                .build();
    }

    private ForeignKeyDefinition.Constraint parseConstraint() {
        if (referenceSituation != null) {
            if (referenceOption.getValue() == ReferencesOptionEnum.CASCADE) {
                return ForeignKeyDefinition.Constraint.CASCADE;
            } else if (referenceOption.getValue() == ReferencesOptionEnum.SET_NULL) {
                return ForeignKeyDefinition.Constraint.SET_NULL;
            }
        }
        return null;
    }

    public enum ReferencesSituationEnum {
        ON_DELETE("ON DELETE");

        private String digest;

        ReferencesSituationEnum(String digest) {
            this.digest = digest;
        }

        @Override
        public String toString() {
            return digest;
        }

        public SqlLiteral symbol(SqlParserPos pos) {
            return SqlLiteral.createSymbol(this, pos);
        }
    }

    public enum ReferencesOptionEnum {
        CASCADE("CASCADE"),
        SET_NULL("SET NULL");

        private String digest;

        ReferencesOptionEnum(String digest) {
            this.digest = digest;
        }

        @Override
        public String toString() {
            return digest;
        }
        /**
         * Creates a parse-tree node representing an occurrence of this keyword at a particular
         * position in the parsed text.
         */
        public SqlLiteral symbol(SqlParserPos pos) {
            return SqlLiteral.createSymbol(this, pos);
        }
    }
}
