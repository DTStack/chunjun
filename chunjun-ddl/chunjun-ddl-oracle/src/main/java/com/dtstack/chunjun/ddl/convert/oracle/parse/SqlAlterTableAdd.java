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

import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;

/** use for add table column/constraint */
public class SqlAlterTableAdd extends SqlAlterTable {

    private final SqlNodeList alterTableAddList;

    public SqlAlterTableAdd(
            SqlParserPos pos, SqlIdentifier tableIdentifier, SqlNodeList alterTableAddList) {
        super(pos, tableIdentifier);
        this.alterTableAddList = alterTableAddList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        for (int i = 0; i < alterTableAddList.size(); i++) {
            writer.keyword("ADD");
            alterTableAddList.get(i).unparse(writer, leftPrec, rightPrec);
        }
    }

    @Override
    public List<DdlOperator> parseToChunjunOperator() {
        List<DdlOperator> res = new ArrayList<>();
        for (SqlNode sqlNode : alterTableAddList) {
            if (sqlNode instanceof SqlConstraint) {
                res.add(
                        ((SqlConstraint) sqlNode)
                                .parseToChunjunOperator(
                                        convertSqlIdentifierToTableIdentifier(tableIdentifier)));
            } else {
                SqlAlterTable add = (SqlAlterTable) sqlNode;
                res.addAll(add.parseToChunjunOperator());
            }
        }
        return res;
    }
}
