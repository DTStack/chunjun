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

package com.dtstack.chunjun.ddl.parse.util;

import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.ddl.parse.type.SqlCustomTypeNameSpec;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SqlNodeUtil {

    public static final SqlDialect EMPTY_SQL_DIALECT =
            new CalciteSqlDialect(
                    SqlDialect.EMPTY_CONTEXT
                            .withLiteralQuoteString("")
                            .withLiteralEscapedQuoteString("")
                            .withUnquotedCasing(Casing.UNCHANGED));

    public static Pair<Integer, Integer> getTypePrecisionAndScale(SqlTypeNameSpec sqlTypeNameSpec) {
        if (sqlTypeNameSpec instanceof SqlBasicTypeNameSpec) {
            SqlBasicTypeNameSpec type = (SqlBasicTypeNameSpec) sqlTypeNameSpec;
            return Pair.of(
                    type.getPrecision() == -1 ? null : type.getPrecision(),
                    type.getScale() == -1 ? null : type.getScale());
        } else if (sqlTypeNameSpec instanceof SqlCustomTypeNameSpec) {
            SqlCustomTypeNameSpec type = (SqlCustomTypeNameSpec) sqlTypeNameSpec;
            return Pair.of(
                    Objects.isNull(type.precision) ? null : type.precision,
                    Objects.isNull(type.scale) ? null : type.scale);
        }
        return Pair.of(null, null);
    }

    public static TableIdentifier convertSqlIdentifierToTableIdentifier(
            SqlIdentifier sqlIdentifier) {
        if (sqlIdentifier.isSimple()) {
            return new TableIdentifier(null, null, sqlIdentifier.getSimple());
        }
        if (sqlIdentifier.names.size() == 2) {
            return new TableIdentifier(
                    null, sqlIdentifier.names.get(0), sqlIdentifier.names.get(1));
        }

        if (sqlIdentifier.names.size() == 3) {
            return new TableIdentifier(
                    sqlIdentifier.names.get(0),
                    sqlIdentifier.names.get(1),
                    sqlIdentifier.names.get(2));
        }

        throw new IllegalArgumentException(
                "convert sqlIdentifier failed, sqlIdentifier: " + sqlIdentifier);
    }

    public static SqlIdentifier convertTableIdentifierToSqlIdentifier(
            TableIdentifier tableIdentifier) {

        ArrayList<String> strings = new ArrayList<>();
        if (tableIdentifier.getDataBase() != null) {
            strings.add(tableIdentifier.getDataBase());
        }
        if (tableIdentifier.getSchema() != null) {
            strings.add(tableIdentifier.getSchema());
        }

        if (tableIdentifier.getTable() != null) {
            strings.add(tableIdentifier.getTable());
        }

        return new SqlIdentifier(strings, SqlParserPos.ZERO);
    }

    public static String getSqlCharStringLiteralString(SqlNode sqlNode, SqlDialect sqlDialect) {
        if (sqlNode instanceof SqlCharStringLiteral) {
            Object value = ((SqlCharStringLiteral) sqlNode).getValue();
            if (value instanceof NlsString) {
                NlsString string = (NlsString) value;
                return string.getValue();
            }
        }
        return null;
    }

    public static String getSqlString(SqlNode sqlNode) {
        return getSqlString(sqlNode, EMPTY_SQL_DIALECT);
    }

    public static String getSqlString(SqlNode sqlNode, SqlDialect sqlDialect) {
        if (sqlNode instanceof SqlCharStringLiteral) {
            Object value = ((SqlCharStringLiteral) sqlNode).getValue();
            if (value instanceof NlsString) {
                NlsString string = (NlsString) value;
                return string.getValue();
            }
        }
        return sqlNode.toSqlString(sqlDialect).getSql();
    }

    public static boolean unparseSqlCharStringLiteral(
            SqlNode sqlNode, SqlWriter writer, int leftPrec, int rightPrec) {
        if (sqlNode instanceof SqlCharStringLiteral) {
            Object value = ((SqlCharStringLiteral) sqlNode).getValue();
            if (value instanceof NlsString) {
                NlsString nlsString = (NlsString) value;
                writer.literal("'" + nlsString.getValue() + "'");
                return true;
            }
        }
        return false;
    }

    public static List<String> parseNodeListString(SqlNodeList nodeList) {
        return parseNodeListString(nodeList, EMPTY_SQL_DIALECT);
    }

    public static List<String> parseNodeListString(SqlNodeList sqlNodeList, SqlDialect sqlDialect) {
        List<String> result = new ArrayList<>();
        if (sqlNodeList == null) {
            return result;
        }
        for (SqlNode sqlNode : sqlNodeList.getList()) {
            result.add(sqlNode.toSqlString(sqlDialect).getSql());
        }
        return result;
    }
}
