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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlIndexOption extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("INDEX_OPTION", SqlKind.COLUMN_DECL);

    public SqlNode keyBlockSize;
    public SqlNode indexType;
    public SqlNode withParse;
    public SqlNode comment;
    public SqlLiteral visiable;
    public SqlNode engineAttribute;
    public SqlNode secondEngineAttribute;

    public SqlIndexOption(
            SqlParserPos pos,
            SqlNode keyBlockSize,
            SqlNode indexType,
            SqlNode withParse,
            SqlNode comment,
            SqlLiteral visiable,
            SqlNode engineAttribute,
            SqlNode secondEngineAttribute) {
        super(pos);
        this.keyBlockSize = keyBlockSize;
        this.indexType = indexType;
        this.withParse = withParse;
        this.comment = comment;
        this.visiable = visiable;
        this.engineAttribute = engineAttribute;
        this.secondEngineAttribute = secondEngineAttribute;
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
        if (keyBlockSize != null) {
            writer.keyword("KEY_BLOCK_SIZE");
            writer.keyword("=");
            keyBlockSize.unparse(writer, leftPrec, rightPrec);
        }
        if (indexType != null) {
            indexType.unparse(writer, leftPrec, rightPrec);
        }
        if (withParse != null) {
            writer.keyword("WITH");
            writer.keyword("PARSER");
            withParse.unparse(writer, leftPrec, rightPrec);
        }
        if (comment != null) {
            writer.keyword("COMMENT");
            if (!SqlNodeUtil.unparseSqlCharStringLiteral(comment, writer, leftPrec, rightPrec)) {
                comment.unparse(writer, leftPrec, rightPrec);
            }
        }
        if (visiable != null) {
            visiable.unparse(writer, leftPrec, rightPrec);
        }
        if (engineAttribute != null) {
            writer.keyword("ENGINE_ATTRIBUTE");
            writer.keyword("=");
            engineAttribute.unparse(writer, leftPrec, rightPrec);
        }
        if (secondEngineAttribute != null) {
            writer.keyword("SECONDARY_ENGINE_ATTRIBUTE");
            writer.keyword("=");
            secondEngineAttribute.unparse(writer, leftPrec, rightPrec);
        }
    }
}
