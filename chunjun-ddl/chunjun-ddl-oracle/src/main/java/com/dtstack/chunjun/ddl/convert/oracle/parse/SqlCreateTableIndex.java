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
import com.dtstack.chunjun.cdc.ddl.definition.IndexDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.IndexOperator;
import com.dtstack.chunjun.cdc.ddl.definition.IndexType;
import com.dtstack.chunjun.ddl.convert.oracle.util.OracleSqlNodeParseUtil;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.convertSqlIdentifierToTableIdentifier;
import static com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil.getSqlString;

public class SqlCreateTableIndex extends SqlCreateIndex {

    private final SqlIdentifier tableIdentifier;
    private final SqlIdentifier aliasIdentifier;
    private final SqlNodeList columnList;

    public SqlCreateTableIndex(
            SqlParserPos pos,
            IndexType indexType,
            SqlIdentifier indexIdentifier,
            SqlIdentifier tableIdentifier,
            SqlIdentifier aliasIdentifier,
            SqlNodeList columnList) {
        super(pos, indexType, indexIdentifier);
        this.tableIdentifier = tableIdentifier;
        this.aliasIdentifier = aliasIdentifier;
        this.columnList = columnList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        tableIdentifier.unparse(writer, leftPrec, rightPrec);
        if (aliasIdentifier != null) {
            aliasIdentifier.unparse(writer, leftPrec, rightPrec);
        }
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.OTHER, "(", ")");
        OracleSqlNodeParseUtil.unParseSqlNodeList(columnList, writer, leftPrec, rightPrec);
        writer.newlineAndIndent();
        writer.endList(frame);
    }

    @Override
    public IndexOperator parseToChunjunOperator() {
        List<IndexDefinition.ColumnInfo> columnInfoList = new ArrayList<>(columnList.size());
        String prefix = "";
        if (aliasIdentifier != null) {
            prefix = getSqlString(aliasIdentifier) + ".";
        }
        for (SqlNode sqlNode : columnList) {
            columnInfoList.add(
                    new IndexDefinition.ColumnInfo(getSqlString(sqlNode).replace(prefix, ""), 0));
        }
        IndexDefinition indexDefinition =
                new IndexDefinition.Builder()
                        .indexType(indexType)
                        .indexName(getSqlString(indexIdentifier))
                        .columns(columnInfoList)
                        .build();
        return new IndexOperator.Builder()
                .type(EventType.ADD_INDEX)
                .sql(this.toSqlString(OracleSqlDialect.DEFAULT).getSql())
                .tableIdentifier(convertSqlIdentifierToTableIdentifier(tableIdentifier))
                .index(indexDefinition)
                .build();
    }
}
