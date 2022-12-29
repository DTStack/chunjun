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

package com.dtstack.chunjun.ddl.convert.oracle.util;

import com.dtstack.chunjun.cdc.ddl.ColumnTypeConvert;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnTypeDesc;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.DataSourceFunction;
import com.dtstack.chunjun.cdc.ddl.definition.ForeignKeyDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.IndexDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.IndexType;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class OracleSqlNodeParseUtil {
    public static String DEFAULT_PRIMARY_KKY_prefix = "flinkx_pk";

    public static String parseCreateDefinition(
            List<ColumnDefinition> columnList,
            List<ConstraintDefinition> constraintList,
            SqlDialect sqlDialect,
            ColumnTypeConvert columnTypeConvert) {
        if (CollectionUtils.isEmpty(columnList) && CollectionUtils.isEmpty(constraintList)) {
            return null;
        }

        StringBuilder sb = new StringBuilder().append("(");

        ArrayList<String> strings = new ArrayList<String>();
        if (CollectionUtils.isNotEmpty(columnList)) {
            columnList.forEach(
                    i ->
                            strings.add(
                                    parseColumnDefinitionToString(
                                            i, sqlDialect, columnTypeConvert)));
        }

        if (CollectionUtils.isNotEmpty(constraintList)) {
            List<ConstraintDefinition> primary =
                    constraintList.stream()
                            .filter(ConstraintDefinition::isPrimary)
                            .collect(Collectors.toList());
            List<ConstraintDefinition> unique =
                    constraintList.stream()
                            .filter(ConstraintDefinition::isUnique)
                            .collect(Collectors.toList());
            List<ConstraintDefinition> skip = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(primary) && CollectionUtils.isNotEmpty(unique)) {
                skip =
                        unique.stream()
                                .filter(
                                        i ->
                                                primary.stream()
                                                        .anyMatch(
                                                                p ->
                                                                        p.getColumns()
                                                                                .equals(
                                                                                        i
                                                                                                .getColumns())))
                                .collect(Collectors.toList());
            }

            List<ConstraintDefinition> finalSkip = skip;
            constraintList.stream()
                    .filter(i -> !finalSkip.contains(i))
                    .forEach(i -> strings.add(parseConstraintDefinitionToString(i, sqlDialect)));
        }
        sb.append(String.join(",", strings));
        sb.append(")");
        return sb.toString();
    }

    public static String parseColumnDefinitionToString(
            ColumnDefinition columnDefinition,
            SqlDialect sqlDialect,
            ColumnTypeConvert columnTypeConvert) {
        StringBuilder sb = new StringBuilder(256);

        sb.append(sqlDialect.quoteIdentifier(columnDefinition.getName())).append(" ");
        if (columnDefinition.getType() != null) {
            String typeName =
                    columnTypeConvert.conventColumnTypeToDatSourceType(columnDefinition.getType());
            sb.append(typeName);

            ColumnTypeDesc columnTypeDesc =
                    columnTypeConvert.getColumnTypeDesc(columnDefinition.getType());
            if (columnDefinition.getPrecision() != null) {
                Integer precision = columnDefinition.getPrecision();
                if (null != columnTypeDesc) {
                    precision = columnTypeDesc.getPrecision(precision);
                }
                if (precision != null) {
                    sb.append("(").append(precision);
                    Integer scale = columnDefinition.getScale();
                    if (scale != null) {
                        if (null != columnTypeDesc) {
                            scale = columnTypeDesc.getScale(scale);
                        }
                        if (scale != null) {
                            sb.append(",").append(scale);
                        }
                    }
                    sb.append(")");
                }

            } else if (null != columnTypeDesc) {
                if (columnTypeDesc.getDefaultConvertPrecision() != null) {
                    sb.append("(").append(columnTypeDesc.getDefaultConvertPrecision());
                    if (columnTypeDesc.getDefaultConvertScale(
                                    columnTypeDesc.getDefaultConvertPrecision())
                            != null) {
                        sb.append(",")
                                .append(
                                        columnTypeDesc.getDefaultConvertScale(
                                                columnTypeDesc.getDefaultConvertPrecision()));
                    }
                    sb.append(")");
                }
            }
        }

        sb.append(" ");

        appendDefaultOption(columnDefinition, sb);

        if (columnDefinition.isNullable() != null && !columnDefinition.isNullable()) {
            sb.append("NOT NULL").append(" ");
        }

        if (columnDefinition.isPrimary() != null && columnDefinition.isPrimary()) {
            sb.append("PRIMARY KEY").append(" ");
        } else if (columnDefinition.isUnique() != null && columnDefinition.isUnique()) {
            sb.append("UNIQUE").append(" ");
        }
        return sb.toString();
    }

    public static String parseConstraintDefinitionToString(
            ConstraintDefinition constraintDefinition, SqlDialect sqlDialect) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("CONSTRAINT").append(" ");
        if (!constraintDefinition.isPrimary()
                && StringUtils.isBlank(constraintDefinition.getName())) {
            throw new IllegalArgumentException("constraint must has name");
        }
        if (constraintDefinition.isPrimary()
                && StringUtils.isBlank(constraintDefinition.getName())) {
            sb.append(
                            DEFAULT_PRIMARY_KKY_prefix
                                    + UUID.randomUUID()
                                            .toString()
                                            .trim()
                                            .replace("-", "")
                                            .substring(0, 12))
                    .append(" ");
        } else {
            sb.append(sqlDialect.quoteIdentifier(constraintDefinition.getName())).append(" ");
        }

        if (constraintDefinition instanceof ForeignKeyDefinition) {
            sb.append("FOREIGN KEY").append(" ");

            sb.append("(").append(" ");
            sb.append(
                    constraintDefinition.getColumns().stream()
                            .map(sqlDialect::quoteIdentifier)
                            .collect(Collectors.joining(",")));
            sb.append(")").append(" ");
            ForeignKeyDefinition foreignKeyDefinition = (ForeignKeyDefinition) constraintDefinition;
            sb.append("REFERENCES")
                    .append(" ")
                    .append(
                            sqlDialect.quoteIdentifier(
                                    foreignKeyDefinition.getReferenceTable().getTable()))
                    .append(" (");
            sb.append(
                            foreignKeyDefinition.getReferenceColumns().stream()
                                    .map(sqlDialect::quoteIdentifier)
                                    .collect(Collectors.joining(",")))
                    .append(") ");

            if (foreignKeyDefinition.getOnDelete() != null
                    && foreignKeyConstraintTotOracleString(foreignKeyDefinition.getOnDelete())
                            != null) {
                sb.append("ON DELETE")
                        .append(" ")
                        .append(
                                foreignKeyConstraintTotOracleString(
                                        foreignKeyDefinition.getOnDelete()));
            }
            return sb.toString();
        }

        if (constraintDefinition.isCheck()
                && StringUtils.isNotBlank(constraintDefinition.getCheck())) {

            sb.append("CHECK").append(" ").append("(");
            sb.append(constraintDefinition.getCheck()).append(" ").append(")");
            return sb.toString();
        }

        if (constraintDefinition.isUnique() || constraintDefinition.isPrimary()) {

            if (constraintDefinition.isUnique()) {
                sb.append("UNIQUE").append(" ");
            }

            if (constraintDefinition.isPrimary()) {
                sb.append("PRIMARY KEY").append(" ");
            }

            sb.append("(").append(" ");
            sb.append(
                    constraintDefinition.getColumns().stream()
                            .map(sqlDialect::quoteIdentifier)
                            .collect(Collectors.joining(",")));
            sb.append(")").append(" ");
        }

        return sb.toString();
    }

    public static String parseCreateIndexDefinitionToString(
            IndexDefinition indexDefinition, SqlIdentifier sqlIdentifier, SqlDialect sqlDialect) {
        StringBuilder sb = new StringBuilder(256).append("CREATE ");
        if (indexDefinition.getIndexType() != null
                && indexDefinition.getIndexType() == IndexType.UNIQUE) {
            sb.append("UNIQUE").append(" ");
        }
        sb.append("INDEX").append(" ");

        if (indexDefinition.getIndexName() != null) {
            sb.append(sqlDialect.quoteIdentifier(indexDefinition.getIndexName())).append(" ");
        }

        sb.append("ON")
                .append(" ")
                .append(sqlIdentifier.toSqlString(sqlDialect).getSql())
                .append(" ");

        if (CollectionUtils.isNotEmpty(indexDefinition.getColumns())) {
            sb.append("(").append(" ");
            sb.append(
                    indexDefinition.getColumns().stream()
                            .map(i -> sqlDialect.quoteIdentifier(i.getName()))
                            .collect(Collectors.joining(",")));
            sb.append(")").append(" ");
        }

        return sb.toString();
    }

    public static String parseComment(
            String commentContent,
            SqlIdentifier sqlIdentifier,
            boolean isColumn,
            SqlDialect sqlDialect) {
        if (isColumn) {
            return "COMMENT ON COLUMN "
                    + sqlIdentifier.toSqlString(sqlDialect).getSql()
                    + " IS "
                    + "'"
                    + commentContent
                    + "'";
        } else {
            return "COMMENT ON TABLE "
                    + sqlIdentifier.toSqlString(sqlDialect).getSql()
                    + " IS "
                    + "'"
                    + commentContent
                    + "'";
        }
    }

    private static String foreignKeyConstraintTotOracleString(
            ForeignKeyDefinition.Constraint constraint) {
        String s = null;
        switch (constraint) {
            case CASCADE:
                s = "CASCADE";
                break;
            case SET_NULL:
                s = "SET NULL";
                break;
        }
        return s;
    }

    public static void appendDefaultOption(ColumnDefinition columnDefinition, StringBuilder sb) {
        if (columnDefinition.getDefaultValue() != null) {
            sb.append("DEFAULT ")
                    .append("'")
                    .append(columnDefinition.getDefaultValue())
                    .append("' ");
        } else if (columnDefinition.getDefaultFunction() != null) {
            sb.append("DEFAULT ")
                    .append(dataSourceFunctionString(columnDefinition.getDefaultFunction()))
                    .append(" ");
        }
    }

    public static String dataSourceFunctionString(DataSourceFunction dataSourceFunction) {
        return dataSourceFunction.name();
    }

    public static void unParseSqlNodeList(
            SqlNodeList sqlNodes, SqlWriter writer, int leftPrec, int rightPrec) {
        if (CollectionUtils.isEmpty(sqlNodes.getList())) {
            return;
        }
        for (SqlNode node : sqlNodes.getList()) {
            writer.sep(",", false);
            writer.newlineAndIndent();
            writer.print("  ");
            node.unparse(writer, leftPrec, rightPrec);
        }
    }
}
