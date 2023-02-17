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
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.getSqlString;
import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.parseNodeListString;

public class SqlConstraint extends SqlCall {

    private static final SqlSpecialOperator UNIQUE =
            new SqlSpecialOperator("UNIQUE", SqlKind.UNIQUE);

    protected static final SqlSpecialOperator PRIMARY =
            new SqlSpecialOperator("PRIMARY KEY", SqlKind.PRIMARY_KEY);

    protected static final SqlSpecialOperator FOREIGN_KEY =
            new SqlSpecialOperator("FOREIGN KEY", SqlKind.FOREIGN_KEY);

    protected static final SqlSpecialOperator CHECK =
            new SqlSpecialOperator("CHECK", SqlKind.CHECK);

    protected static final SqlSpecialOperator REFERENCES =
            new SqlSpecialOperator("REFERENCES", SqlKind.CHECK);

    protected static final SqlSpecialOperator NOT_NULL =
            new SqlSpecialOperator("NOT NULL", SqlKind.CHECK);

    private final SqlIdentifier name;
    private final SqlNodeList columns;
    private final SqlLiteral constraintSpec;
    private final SqlConstraintReferences references;
    private final SqlNode checkCondition;

    public SqlConstraint(
            SqlParserPos pos,
            SqlIdentifier name,
            SqlNodeList columns,
            SqlLiteral constraintSpec,
            SqlConstraintReferences references,
            SqlNode checkCondition) {
        super(pos);
        this.name = name;
        this.columns = columns;
        this.constraintSpec = constraintSpec;
        this.references = references;
        this.checkCondition = checkCondition;
    }

    @NotNull
    @Override
    public SqlOperator getOperator() {
        if (SqlConstraintSpec.UNIQUE.name().equalsIgnoreCase(constraintSpec.toValue())) {
            return UNIQUE;
        }
        if (SqlConstraintSpec.PRIMARY_KEY.name().equalsIgnoreCase(constraintSpec.toValue())) {
            return PRIMARY;
        }
        if (SqlConstraintSpec.FOREIGN_KEY.name().equalsIgnoreCase(constraintSpec.toValue())) {
            return FOREIGN_KEY;
        }
        if (SqlConstraintSpec.CHECK.name().equalsIgnoreCase(constraintSpec.toValue())) {
            return CHECK;
        }
        if (SqlConstraintSpec.REFERENCES.name().equalsIgnoreCase(constraintSpec.toValue())) {
            return REFERENCES;
        }
        if (SqlConstraintSpec.NOT_NULL.name().equalsIgnoreCase(constraintSpec.toValue())) {
            return NOT_NULL;
        }
        throw new IllegalArgumentException("not contains " + constraintSpec.toValue() + "operator");
    }

    public boolean isNotNull() {
        return SqlConstraintSpec.NOT_NULL.toString().equalsIgnoreCase(constraintSpec.toValue());
    }

    @NotNull
    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlNodeList getColumns() {
        return columns;
    }

    public SqlLiteral getConstraintSpec() {
        return constraintSpec;
    }

    public SqlConstraintReferences getReferences() {
        return references;
    }

    public SqlNode getCheckCondition() {
        return checkCondition;
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec, boolean isInlineConstarint) {
        if (name != null) {
            writer.keyword("CONSTRAINT");
            name.unparse(writer, 0, 0);
        }

        if (constraintSpec != null) {
            writer.keyword(constraintSpec.toValue());
        }

        if (columns != null && !isInlineConstarint) {
            writer.keyword("(");
            columns.unparse(writer, 0, 0);
            writer.keyword(")");
        }
        if (references != null) {
            references.unparse(writer, 0, 0);
        }
        if (checkCondition != null) {
            writer.keyword("(");
            checkCondition.unparse(writer, 0, 0);
            writer.keyword(")");
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        unparse(writer, leftPrec, rightPrec, false);
    }

    public ConstraintDefinition parseToConstraintDefinition() {
        String constraintName = name == null ? null : getSqlString(name);
        List<String> columnNameList = parseNodeListString(columns);
        if (SqlConstraintSpec.FOREIGN_KEY.name().equalsIgnoreCase(constraintSpec.toValue())
                || SqlConstraintSpec.REFERENCES.name().equalsIgnoreCase(constraintSpec.toValue())) {
            return references.parseToForeignKeyDefinition(constraintName, columnNameList);
        }

        boolean isPrimaryKey =
                SqlConstraintSpec.PRIMARY_KEY.toString().equalsIgnoreCase(constraintSpec.toValue());
        boolean isUnique =
                SqlConstraintSpec.UNIQUE.toString().equalsIgnoreCase(constraintSpec.toValue());
        boolean isCheck = SqlConstraintSpec.CHECK.name().equalsIgnoreCase(constraintSpec.toValue());
        String check = null;
        if (isCheck) {
            check = getSqlString(checkCondition);
        }
        return new ConstraintDefinition.Builder()
                .name(constraintName)
                .columns(columnNameList)
                .isPrimary(isPrimaryKey)
                .isUnique(isUnique)
                .isCheck(isCheck)
                .check(check)
                .build();
    }

    public DdlOperator parseToChunjunOperator(TableIdentifier tableIdentifier) {
        return new ConstraintOperator.Builder()
                .type(EventType.ADD_CONSTRAINT)
                .sql(this.toSqlString(OracleSqlDialect.DEFAULT).getSql())
                .tableIdentifier(tableIdentifier)
                .constraintDefinition(parseToConstraintDefinition())
                .build();
    }

    /** Enumeration of SQL unique specification. */
    public enum SqlConstraintSpec {
        NOT_NULL("NOT NULL"),
        NULL("NULL"),
        PRIMARY_KEY("PRIMARY KEY"),
        UNIQUE("UNIQUE"),
        UNIQUE_KEY("UNIQUE KEY"),
        UNIQUE_INDEX("UNIQUE INDEX"),
        FOREIGN_KEY("FOREIGN KEY"),
        CHECK("CHECK"),
        REFERENCES("REFERENCES");

        private final String digest;

        SqlConstraintSpec(String digest) {
            this.digest = digest;
        }

        public String getDigest() {
            return digest;
        }

        @Override
        public String toString() {
            return this.digest;
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
