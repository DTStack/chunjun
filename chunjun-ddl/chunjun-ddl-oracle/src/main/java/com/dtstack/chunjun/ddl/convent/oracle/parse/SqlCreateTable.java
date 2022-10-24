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

package com.dtstack.chunjun.ddl.convent.oracle.parse;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.commons.collections.CollectionUtils;

import javax.annotation.Nonnull;

import java.util.List;

public class SqlCreateTable extends SqlCreate {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    public SqlIdentifier name;

    public final SqlNodeList columnList;

    public SqlNodeList constraintList;
    public SqlNodeList checkConstraintList;

    public SqlCreateTable(
            SqlParserPos pos,
            SqlIdentifier name,
            SqlNodeList columnList,
            SqlNodeList constraintList,
            SqlNodeList checkConstraintList) {
        super(OPERATOR, pos, false, false);
        this.name = name;
        this.columnList = columnList;
        this.constraintList = constraintList;
        this.checkConstraintList = checkConstraintList;
    }

    @Override
    @Nonnull
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, constraintList, checkConstraintList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("TABLE");

        name.unparse(writer, leftPrec, rightPrec);

        if (columnList.size() > 0 || constraintList.size() > 0) {
            SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");

            unPaseSqlNodeList(columnList, writer, leftPrec, rightPrec);
            unPaseSqlNodeList(constraintList, writer, leftPrec, rightPrec);

            writer.newlineAndIndent();
            writer.endList(frame);
        }
    }

    protected void unPaseSqlNodeList(
            SqlNodeList sqlNodes, SqlWriter writer, int leftPrec, int rightPrec) {
        if (CollectionUtils.isEmpty(sqlNodes.getList())) {
            return;
        }
        for (SqlNode node : sqlNodes.getList()) {
            writer.sep(",", false);
            writer.newlineAndIndent();
            writer.print("  ");
            node.unparse(writer, leftPrec, rightPrec);
        }
    }
}
