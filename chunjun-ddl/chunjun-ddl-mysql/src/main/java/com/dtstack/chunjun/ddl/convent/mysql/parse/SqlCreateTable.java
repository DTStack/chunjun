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

import java.util.List;

public class SqlCreateTable extends SqlCreate {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    public final boolean isTemporary;

    public SqlIdentifier name;

    public boolean likeTable;
    public SqlIdentifier originalName;

    public final SqlNodeList columnList;

    public SqlNodeList indexList;

    public SqlNodeList constraintList;
    public SqlNodeList checkConstraintList;

    public SqlNodeList tableOptions;

    public SqlCreateTable(
            SqlParserPos pos,
            boolean replace,
            boolean isTemporary,
            SqlIdentifier name,
            boolean ifNotExists,
            boolean likeTable,
            SqlIdentifier originalName,
            SqlNodeList columnList,
            SqlNodeList indexList,
            SqlNodeList constraintList,
            SqlNodeList checkConstraintList,
            SqlNodeList tableOptions) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.isTemporary = isTemporary;
        this.name = name;
        this.likeTable = likeTable;
        this.originalName = originalName;
        this.columnList = columnList;
        this.tableOptions = tableOptions;
        this.indexList = indexList;
        this.constraintList = constraintList;
        this.checkConstraintList = checkConstraintList;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                name,
                originalName,
                columnList,
                indexList,
                constraintList,
                checkConstraintList,
                tableOptions);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (isTemporary) {
            writer.keyword("TEMPORARY");
        }
        writer.keyword("TABLE");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS]");
        }
        name.unparse(writer, leftPrec, rightPrec);

        if (likeTable) {
            writer.keyword("LIKE");
            originalName.unparse(writer, leftPrec, rightPrec);
        } else {
            if (columnList.size() > 0 || indexList.size() > 0 || constraintList.size() > 0) {
                SqlWriter.Frame frame =
                        writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");

                unPaseSqlNodeList(columnList, writer, leftPrec, rightPrec);
                unPaseSqlNodeList(indexList, writer, leftPrec, rightPrec);
                unPaseSqlNodeList(constraintList, writer, leftPrec, rightPrec);

                writer.newlineAndIndent();
                writer.endList(frame);
            }

            if (tableOptions != null && tableOptions.getList().size() > 0) {
                for (SqlNode sqlNode : tableOptions) {
                    sqlNode.unparse(writer, leftPrec, rightPrec);
                    writer.print("  ");
                }
            }
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
