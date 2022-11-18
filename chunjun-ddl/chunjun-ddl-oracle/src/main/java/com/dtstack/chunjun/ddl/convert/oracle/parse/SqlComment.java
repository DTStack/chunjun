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
import com.dtstack.chunjun.cdc.ddl.definition.ColumnDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;
import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.getSqlString;

public class SqlComment extends SqlCall {

    private static final SqlSpecialOperator COMMENT =
            new SqlSpecialOperator("COMMENT", SqlKind.OTHER_DDL);
    private final SqlIdentifier commentIdentifier;
    private final SqlLiteral commentType;
    private final String comment;

    public SqlComment(
            SqlParserPos pos,
            SqlIdentifier commentIdentifier,
            SqlLiteral commentType,
            String comment) {
        super(pos);
        this.commentIdentifier = commentIdentifier;
        this.commentType = commentType;
        this.comment = comment;
    }

    @NotNull
    @Override
    public SqlOperator getOperator() {
        return COMMENT;
    }

    @NotNull
    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("COMMENT ON");
        commentType.unparse(writer, leftPrec, rightPrec);
        commentIdentifier.unparse(writer, leftPrec, rightPrec);
        writer.keyword("IS");
        writer.literal(comment);
    }

    public SqlIdentifier getCommentIdentifier() {
        return commentIdentifier;
    }

    public SqlLiteral getCommentType() {
        return commentType;
    }

    public String getComment() {
        return comment;
    }

    public DdlOperator parseToChunjunOperator() {
        if (commentType.toValue().equals("COLUMN")) {
            return parseAlterColumnComment();
        } else {
            return parseAlterTableComment();
        }
    }

    private DdlOperator parseAlterColumnComment() {
        EventType eventType = EventType.ALTER_COLUMN;
        String identifierSql = getSqlString(commentIdentifier);
        String[] identifiers = identifierSql.split("\\.");
        TableIdentifier tableIdentifier;
        if (identifiers.length == 2) {
            tableIdentifier = new TableIdentifier(null, null, identifiers[0]);
        } else {
            tableIdentifier = new TableIdentifier(null, identifiers[0], identifiers[1]);
        }
        ColumnDefinition columnDefinition =
                new ColumnDefinition.Builder()
                        .name(identifiers[identifiers.length - 1])
                        .comment(comment)
                        .build();
        return new ColumnOperator.Builder()
                .type(eventType)
                .sql(this.toSqlString(OracleSqlDialect.DEFAULT).getSql())
                .tableIdentifier(tableIdentifier)
                .columns(Collections.singletonList(columnDefinition))
                .build();
    }

    private DdlOperator parseAlterTableComment() {
        TableIdentifier tableIdentifier = convertSqlIdentifierToTableIdentifier(commentIdentifier);
        EventType eventType = EventType.ALTER_TABLE_COMMENT;
        return new TableOperator.Builder()
                .type(eventType)
                .sql(this.toSqlString(OracleSqlDialect.DEFAULT).getSql())
                .tableIdentifier(tableIdentifier)
                .comment(comment)
                .build();
    }

    public enum CommentTypeEnum {
        COLUMN("COLUMN"),
        TABLE("TABLE");

        private final String type;

        CommentTypeEnum(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return type;
        }

        public SqlLiteral symbol(SqlParserPos pos) {
            return SqlLiteral.createSymbol(this, pos);
        }
    }
}
