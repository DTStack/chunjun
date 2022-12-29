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

/** use for drop table column/constraint */
public class SqlAlterTableDrop extends SqlAlterTable {

    private final SqlNodeList alterTableDropList;

    public SqlAlterTableDrop(
            SqlParserPos pos, SqlIdentifier tableIdentifier, SqlNodeList alterTableAddList) {
        super(pos, tableIdentifier);
        this.alterTableDropList = alterTableAddList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        for (int i = 0; i < alterTableDropList.size(); i++) {
            writer.keyword("DROP");
            alterTableDropList.get(i).unparse(writer, leftPrec, rightPrec);
        }
    }

    @Override
    public List<DdlOperator> parseToChunjunOperator() {
        List<DdlOperator> res = new ArrayList<>();
        for (SqlNode sqlNode : alterTableDropList) {
            SqlAlterTable drop = (SqlAlterTable) sqlNode;
            res.addAll(drop.parseToChunjunOperator());
        }
        return res;
    }
}
