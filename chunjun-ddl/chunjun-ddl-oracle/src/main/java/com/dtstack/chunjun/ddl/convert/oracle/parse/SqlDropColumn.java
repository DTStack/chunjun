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
import com.dtstack.chunjun.ddl.convert.oracle.util.OracleSqlNodeParseUtil;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;
import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.getSqlString;

public class SqlDropColumn extends SqlAlterTable {

    private final SqlNodeList columnList;

    public SqlDropColumn(SqlParserPos pos, SqlIdentifier tableIdentifier, SqlNodeList columnList) {
        super(pos, tableIdentifier);
        this.columnList = columnList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        if (columnList.size() == 1) {
            writer.keyword("COLUMN");
            columnList.get(0).unparse(writer, leftPrec, rightPrec);
        } else {
            SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.OTHER, "(", ")");
            OracleSqlNodeParseUtil.unParseSqlNodeList(columnList, writer, leftPrec, rightPrec);
            writer.newlineAndIndent();
            writer.endList(frame);
        }
    }

    @Override
    public List<DdlOperator> parseToChunjunOperator() {
        List<ColumnDefinition> columnDefinitionList = new ArrayList<>();
        for (SqlNode sqlNode : columnList) {
            columnDefinitionList.add(
                    new ColumnDefinition.Builder().name(getSqlString(sqlNode)).build());
        }
        return Collections.singletonList(
                new ColumnOperator.Builder()
                        .type(EventType.DROP_COLUMN)
                        .sql(this.toSqlString(OracleSqlDialect.DEFAULT).getSql())
                        .tableIdentifier(convertSqlIdentifierToTableIdentifier(tableIdentifier))
                        .columns(columnDefinitionList)
                        .build());
    }
}
