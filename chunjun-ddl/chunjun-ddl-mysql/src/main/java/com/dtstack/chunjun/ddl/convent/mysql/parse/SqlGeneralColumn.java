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

import com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlGeneralColumn extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

    public SqlIdentifier name;
    public SqlDataTypeSpec type;
    public final SqlNode expression;
    public final SqlNode defaultValue;
    public final SqlLiteral nullAble;
    public final SqlLiteral visiable;
    public final SqlLiteral autoIncreMent;
    public final SqlLiteral uniqueKey;
    public final SqlLiteral primary;
    public final SqlNode comment;
    public final SqlNode sqlCollation;
    public final SqlLiteral columnFormat;
    public final SqlNode engineAttribute;
    public final SqlNode secondaryEngineAttribute;
    public final SqlLiteral storage;
    public final SqlLiteral stored;
    public SqlNode referenceDefinition = null;
    public final SqlNode constraint;

    public SqlGeneralColumn(
            SqlParserPos pos,
            SqlIdentifier name,
            SqlDataTypeSpec type,
            SqlNode expression,
            SqlNode defaultValue,
            SqlLiteral nullAble,
            SqlLiteral visiable,
            SqlLiteral autoIncreMent,
            SqlLiteral uniqueKey,
            SqlLiteral primary,
            SqlNode comment,
            SqlNode sqlCollation,
            SqlLiteral columnFormat,
            SqlNode engineAttribute,
            SqlNode secondaryEngineAttribute,
            SqlLiteral storage,
            SqlLiteral stored,
            SqlNode referenceDefinition,
            SqlNode constraint) {
        super(pos);
        this.name = name;
        this.type = type;
        this.expression = expression;
        this.defaultValue = defaultValue;
        this.nullAble = nullAble;
        this.visiable = visiable;
        this.autoIncreMent = autoIncreMent;
        this.uniqueKey = uniqueKey;
        this.primary = primary;
        this.comment = comment;
        this.sqlCollation = sqlCollation;
        this.columnFormat = columnFormat;
        this.engineAttribute = engineAttribute;
        this.secondaryEngineAttribute = secondaryEngineAttribute;
        this.storage = storage;
        this.stored = stored;
        this.referenceDefinition = referenceDefinition;
        this.constraint = constraint;
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
        name.unparse(writer, leftPrec, rightPrec);
        type.unparse(writer, leftPrec, rightPrec);
        unparseNode(nullAble, writer, leftPrec, rightPrec);
        if (defaultValue != null) {
            writer.keyword(" DEFAULT ");
            if (!SqlNodeUtil.unparseSqlCharStringLiteral(
                    defaultValue, writer, leftPrec, rightPrec)) {
                defaultValue.unparse(writer, leftPrec, rightPrec);
            }
        }
        unparseNode(visiable, writer, leftPrec, rightPrec);
        unparseNode(autoIncreMent, writer, leftPrec, rightPrec);
        unparseNode(uniqueKey, writer, leftPrec, rightPrec);
        if (primary != null) {
            writer.keyword(" ");
            primary.unparse(writer, leftPrec, rightPrec);
            writer.keyword(" KEY ");
        }
        if (comment != null) {
            writer.keyword(" COMMENT ");
            if (!SqlNodeUtil.unparseSqlCharStringLiteral(comment, writer, leftPrec, rightPrec)) {
                comment.unparse(writer, leftPrec, rightPrec);
            }
        }

        if (sqlCollation != null) {
            writer.keyword(" COLLATE ");
            sqlCollation.unparse(writer, leftPrec, rightPrec);
        }

        if (expression != null) {
            writer.keyword(" AS ");
            expression.unparse(writer, leftPrec, rightPrec);
        }

        unparseNode(stored, writer, leftPrec, rightPrec);

        if (columnFormat != null) {
            writer.keyword(" COLUMN_FORMAT ");
            columnFormat.unparse(writer, leftPrec, rightPrec);
        }
        if (engineAttribute != null) {
            writer.keyword(" ENGINE_ATTRIBUTE ");
            engineAttribute.unparse(writer, leftPrec, rightPrec);
        }
        if (secondaryEngineAttribute != null) {
            writer.keyword(" SECONDARY_ENGINE_ATTRIBUTE ");
            secondaryEngineAttribute.unparse(writer, leftPrec, rightPrec);
        }

        if (storage != null) {
            writer.keyword(" ");
            writer.keyword("STORAGE");
            storage.unparse(writer, leftPrec, rightPrec);
        }

        if (referenceDefinition != null) {
            writer.keyword(" ");
            referenceDefinition.unparse(writer, leftPrec, rightPrec);
        }

        if (constraint != null) {
            writer.keyword(" ");
            constraint.unparse(writer, leftPrec, rightPrec);
        }
    }

    private void unparseNode(SqlNode node, SqlWriter writer, int leftPrec, int rightPrec) {
        if (node != null) {
            writer.keyword(" ");
            node.unparse(writer, leftPrec, rightPrec);
        }
    }
}
