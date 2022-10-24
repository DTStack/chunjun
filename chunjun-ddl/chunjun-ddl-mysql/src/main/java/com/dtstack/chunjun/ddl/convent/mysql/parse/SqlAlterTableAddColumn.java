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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlAlterTableAddColumn extends SqlAlterTableOperator {
    public SqlNodeList columns;
    public SqlLiteral order;
    public SqlIdentifier coordinateColumn;

    public SqlAlterTableAddColumn(
            SqlParserPos pos,
            SqlIdentifier tableIdentifier,
            SqlNodeList columns,
            SqlLiteral order,
            SqlIdentifier coordinateColumn) {
        super(pos, tableIdentifier);
        this.columns = columns;
        this.order = order;
        this.coordinateColumn = coordinateColumn;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableIdentifier, columns, order, coordinateColumn);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        //        writer.keyword("ALTER TABLE");
        //        tableIdentifier.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ADD");
        writer.keyword("COLUMN");

        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode column : columns) {
            printIndent(writer);
            column.unparse(writer, leftPrec, rightPrec);
        }
        writer.newlineAndIndent();
        writer.endList(frame);
        if (order != null) {
            order.unparse(writer, leftPrec, rightPrec);
            coordinateColumn.unparse(writer, leftPrec, rightPrec);
        }
    }

    protected void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }
}
