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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlAlterTableChangeColumn extends SqlAlterTableOperator {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER TABLE CHANGE COLUMN", SqlKind.ALTER_TABLE);

    public SqlIdentifier oldColumnName;
    public SqlNode column;
    public SqlLiteral order;
    public SqlIdentifier coordinateColumn;

    public SqlAlterTableChangeColumn(
            SqlParserPos pos,
            SqlIdentifier tableIdentifier,
            SqlIdentifier oldColumnName,
            SqlNode column,
            SqlLiteral order,
            SqlIdentifier coordinateColumn) {
        super(pos, tableIdentifier);
        this.column = column;
        this.oldColumnName = oldColumnName;
        this.order = order;
        this.coordinateColumn = coordinateColumn;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                tableIdentifier, column, oldColumnName, order, coordinateColumn);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        //        writer.keyword("ALTER TABLE");
        //        tableIdentifier.unparse(writer, leftPrec, rightPrec);
        if (oldColumnName != null) {
            writer.keyword("CHANGE");
            writer.keyword("COLUMN");
            oldColumnName.unparse(writer, leftPrec, rightPrec);
            column.unparse(writer, leftPrec, rightPrec);
            if (order != null) {
                order.unparse(writer, leftPrec, rightPrec);
                coordinateColumn.unparse(writer, leftPrec, rightPrec);
            }
        } else {
            writer.keyword("MODIFY");
            writer.keyword("COLUMN");
            column.unparse(writer, leftPrec, rightPrec);
            if (order != null) {
                order.unparse(writer, leftPrec, rightPrec);
                coordinateColumn.unparse(writer, leftPrec, rightPrec);
            }
        }
    }
}
