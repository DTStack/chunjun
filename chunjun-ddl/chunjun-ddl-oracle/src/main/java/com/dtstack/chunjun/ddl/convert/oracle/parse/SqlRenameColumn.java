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

import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;
import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.getSqlString;

public class SqlRenameColumn extends SqlAlterTable {

    private SqlIdentifier oldColumnName;
    private SqlIdentifier newColumnName;

    public SqlRenameColumn(
            SqlParserPos pos,
            SqlIdentifier tableIdentifier,
            SqlIdentifier oldColumnName,
            SqlIdentifier newColumnName) {
        super(pos, tableIdentifier);
        this.oldColumnName = oldColumnName;
        this.newColumnName = newColumnName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("RENAME COLUMN");
        oldColumnName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("TO");
        newColumnName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public List<DdlOperator> parseToChunjunOperator() {
        ColumnDefinition columnDefinition =
                new ColumnDefinition.Builder().name(getSqlString(oldColumnName)).build();

        return Collections.singletonList(
                new ColumnOperator.Builder()
                        .type(EventType.RENAME_COLUMN)
                        .sql(this.toSqlString(OracleSqlDialect.DEFAULT).getSql())
                        .tableIdentifier(convertSqlIdentifierToTableIdentifier(tableIdentifier))
                        .columns(Collections.singletonList(columnDefinition))
                        .newName(getSqlString(newColumnName))
                        .build());
    }
}
