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

package com.dtstack.chunjun.ddl.convent.mysql.parse.type;

import com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.stream.Collectors;

public class SqlSetTypeNameSpec extends SqlUserDefinedTypeNameSpec {
    public final List<SqlNode> values;

    public SqlSetTypeNameSpec(SqlIdentifier typeName, SqlParserPos pos, List<SqlNode> values) {
        super(typeName, pos);
        this.values = values;
    }

    public SqlSetTypeNameSpec(String name, SqlParserPos pos, List<SqlNode> values) {
        super(name, pos);
        this.values = values;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        String collect =
                values.stream()
                        .map(
                                i -> {
                                    String sqlCharStringLiteralString =
                                            SqlNodeUtil.getSqlCharStringLiteralString(
                                                    i, MysqlSqlDialect.DEFAULT);
                                    if (sqlCharStringLiteralString != null) {
                                        return "'" + sqlCharStringLiteralString + "'";
                                    }
                                    return SqlNodeUtil.getSqlString(i, MysqlSqlDialect.DEFAULT);
                                })
                        .collect(Collectors.joining(","));
        writer.print(getTypeName().getSimple());
        writer.print("(");
        writer.print(collect);
        writer.print(")");
    }
}
