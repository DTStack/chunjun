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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;

public class SqlAlterEvent extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER EVENT", SqlKind.ALTER_TABLE);

    private final SqlIdentifier eventName;
    private final SqlNode schedule;
    private final Boolean isPreserve;
    private final SqlIdentifier user;
    private final SqlIdentifier newEventName;
    private final SqlNode doLiteral;
    private final SqlNode comment;
    private final SqlLiteral enable;

    public SqlAlterEvent(
            SqlParserPos pos,
            SqlIdentifier eventName,
            SqlNode schedule,
            Boolean isPreserve,
            SqlIdentifier user,
            SqlIdentifier newEventName,
            SqlNode doLiteral,
            SqlNode comment,
            SqlLiteral enable) {
        super(pos);
        this.eventName = eventName;
        this.schedule = schedule;
        this.isPreserve = isPreserve;
        this.user = user;
        this.newEventName = newEventName;
        this.doLiteral = doLiteral;
        this.comment = comment;
        this.enable = enable;
    }

    @Override
    @Nonnull
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                eventName, schedule, user, newEventName, doLiteral, comment, enable);
    }

    @Override
    @Nonnull
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPre, int rightPre) {
        writer.keyword("ALTER");
        if (user != null) {
            user.unparse(writer, leftPre, rightPre);
        }
        writer.keyword("EVENT");
        eventName.unparse(writer, leftPre, rightPre);
        if (schedule != null) {
            writer.keyword("ON SCHEDULE");
            schedule.unparse(writer, leftPre, rightPre);
        }
        if (isPreserve != null) {
            if (isPreserve) {
                writer.keyword("ON COMPLETION PRESERVE");
            } else {
                writer.keyword("ON COMPLETION NOT PRESERVE");
            }
        }

        if (newEventName != null) {
            writer.keyword("RENAME TO");
            newEventName.unparse(writer, leftPre, rightPre);
        }
        if (enable != null) {
            enable.unparse(writer, leftPre, rightPre);
        }
        if (comment != null) {
            if (!SqlNodeUtil.unparseSqlCharStringLiteral(comment, writer, leftPre, rightPre)) {
                comment.unparse(writer, leftPre, rightPre);
            }
        }
        if (doLiteral != null) {
            writer.keyword("DO");
            doLiteral.unparse(writer, leftPre, rightPre);
        }
    }
}
