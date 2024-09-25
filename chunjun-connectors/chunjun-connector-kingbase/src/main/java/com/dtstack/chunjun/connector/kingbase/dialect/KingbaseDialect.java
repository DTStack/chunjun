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
package com.dtstack.chunjun.connector.kingbase.dialect;

import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.kingbase.converter.KingbaseRawTypeMapper;
import com.dtstack.chunjun.connector.kingbase.util.KingbaseConstants;
import com.dtstack.chunjun.converter.RawTypeMapper;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class KingbaseDialect implements JdbcDialect {

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of(KingbaseConstants.DRIVER);
    }

    @Override
    public String dialectName() {
        return KingbaseConstants.DB_NAME;
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith(KingbaseConstants.URL_PREFIX);
    }

    @Override
    public RawTypeMapper getRawTypeConverter() {
        return KingbaseRawTypeMapper::apply;
    }

    @Override
    public Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean allReplace) {
        String uniqueColumns =
                Arrays.stream(uniqueKeyFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String updateClause = buildUpdateClause(fieldNames, allReplace);

        return Optional.of(
                getInsertIntoStatement(schema, tableName, fieldNames)
                        + " ON CONFLICT ("
                        + uniqueColumns
                        + ") DO UPDATE SET  "
                        + updateClause);
    }

    /**
     * if allReplace is true: use ISNULL() FUNCTION to handle null values. For example: SET dname =
     * isnull(EXCLUDED.dname,t1.dname) else allReplace is false: SET dname = EXCLUDED.dname
     *
     * @param fieldNames
     * @param allReplace
     * @return
     */
    private String buildUpdateClause(String[] fieldNames, boolean allReplace) {
        String updateClause;
        if (allReplace) {
            updateClause =
                    Arrays.stream(fieldNames)
                            .map(
                                    f ->
                                            quoteIdentifier(f)
                                                    + "=ISNULL(EXCLUDED."
                                                    + quoteIdentifier(f)
                                                    + ", t1."
                                                    + quoteIdentifier(f)
                                                    + ")")
                            .collect(Collectors.joining(", "));
        } else {
            updateClause =
                    Arrays.stream(fieldNames)
                            .map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
                            .collect(Collectors.joining(", "));
        }
        return updateClause;
    }

    /**
     * override: add alias for table which is used in upsert statement
     *
     * @param schema
     * @param tableName
     * @param fieldNames
     * @return
     */
    @Override
    public String getInsertIntoStatement(String schema, String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        return "INSERT INTO "
                + buildTableInfoWithSchema(schema, tableName)
                + " as t1 "
                + "("
                + columns
                + ")"
                + " VALUES ("
                + placeholders
                + ")";
    }

    @Override
    public String getRowNumColumn(String orderBy) {
        return String.format("row_number() over(%s) as CHUNJUN_ROWNUM", orderBy);
    }
}
