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

package com.dtstack.chunjun.ddl.convent.mysql.util;

import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.cdc.ddl.ColumnType;
import com.dtstack.chunjun.cdc.ddl.ColumnTypeConvert;
import com.dtstack.chunjun.cdc.ddl.CustomColumnType;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnOperator;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnTypeDesc;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DataSourceFunction;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.ForeignKeyDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.IndexDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.IndexOperator;
import com.dtstack.chunjun.cdc.ddl.definition.IndexType;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.ddl.convent.mysql.parse.KeyPart;
import com.dtstack.chunjun.ddl.convent.mysql.parse.ReferenceDefinition;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableAlterColumn;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableChangeColumn;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlAlterTableDrop;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlCheckConstraint;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlGeneralColumn;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlIndex;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlIndexOption;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlMysqlConstraintEnable;
import com.dtstack.chunjun.ddl.convent.mysql.parse.SqlTableConstraint;
import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.AlterTableColumnOperatorTypeEnum;
import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.AlterTableTargetTypeEnum;
import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.ReferenceOptionEnums;
import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.ReferenceSituationEnum;
import com.dtstack.chunjun.ddl.convent.mysql.parse.enums.SqlConstraintSpec;
import com.dtstack.chunjun.ddl.parse.type.SqlTypeConvertedNameSpec;
import com.dtstack.chunjun.ddl.parse.util.SqlNodeUtil;
import com.dtstack.chunjun.mapping.MappingRule;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.ddl.convent.mysql.parse.enums.AlterTableTargetTypeEnum.CHECK;
import static com.dtstack.chunjun.ddl.convent.mysql.parse.enums.AlterTableTargetTypeEnum.PRIMARY_kEY;

public class SqlNodeParseUtil {
    public static ColumnDefinition parseColumn(
            SqlGeneralColumn column, ColumnTypeConvert columnTypeConvert) {
        String columnTypeName = column.type.getTypeName().toString();
        ColumnType columnType;
        if (!(column.type.getTypeNameSpec() instanceof SqlTypeConvertedNameSpec)) {
            columnType = columnTypeConvert.conventDataSourceTypeToColumnType(columnTypeName);
        } else {
            columnType = new CustomColumnType(columnTypeName);
        }

        boolean nullAble =
                column.nullAble == null
                        || SqlMysqlConstraintEnable.NULLABLE.equals(column.nullAble.getValue());

        boolean uniqueKey =
                column.uniqueKey != null
                        && SqlMysqlConstraintEnable.UNIQUE.equals(column.uniqueKey.getValue());

        boolean autoIncrement = false;

        //        if (MysqlType.SERIAL.getSourceName().equalsIgnoreCase(columnTypeName)) {
        //            nullAble = false;
        //            uniqueKey = true;
        //            autoIncrement = true;
        //        }

        Pair<Integer, Integer> precisionAndScale =
                SqlNodeUtil.getTypePrecisionAndScale(column.type.getTypeNameSpec());
        String defaultValue = null;
        DataSourceFunction defaultValueFunction = null;
        if (column.defaultValue != null) {
            if (column.defaultValue instanceof SqlIdentifier) {
                defaultValueFunction =
                        dataSourceFunction(((SqlIdentifier) column.defaultValue).getSimple());
            } else {
                defaultValue =
                        SqlNodeUtil.getSqlString(
                                column.defaultValue, SqlNodeUtil.EMPTY_SQL_DIALECT);
            }
        }

        return new ColumnDefinition(
                column.name.getSimple(),
                columnType,
                nullAble,
                column.primary != null
                        && SqlMysqlConstraintEnable.PRIMARY.equals(column.primary.getValue()),
                uniqueKey,
                autoIncrement,
                defaultValue,
                defaultValueFunction,
                column.comment == null
                        ? null
                        : SqlNodeUtil.getSqlString(column.comment, SqlNodeUtil.EMPTY_SQL_DIALECT),
                precisionAndScale.getLeft(),
                precisionAndScale.getRight(),
                column.constraint == null
                        ? null
                        : ((SqlCheckConstraint) column.constraint)
                                .expression
                                .toSqlString(SqlNodeUtil.EMPTY_SQL_DIALECT)
                                .getSql());
    }

    public static IndexDefinition parseIndex(SqlIndex index) {
        IndexType indexType = null;
        if (index.getSpecialType() != null) {
            indexType = IndexType.valueOf(index.getSpecialType().getValue().toString());
        }

        ArrayList<IndexDefinition.ColumnInfo> columnInfos = new ArrayList<>();
        if (index.getKetPartList() != null
                && CollectionUtils.isNotEmpty(index.getKetPartList().getList())) {
            for (SqlNode sqlNode : index.getKetPartList().getList()) {
                columnInfos.add(
                        new IndexDefinition.ColumnInfo(
                                ((KeyPart) sqlNode)
                                        .getColName()
                                        .toSqlString(SqlNodeUtil.EMPTY_SQL_DIALECT)
                                        .getSql(),
                                ((KeyPart) sqlNode).getColumLength() != null
                                        ? Integer.valueOf(
                                                ((KeyPart) sqlNode).getColumLength().toString())
                                        : null));
            }
        }

        Boolean isVisiable = null;
        String comment = null;
        if (index.getIndexOption() != null) {
            SqlIndexOption indexOption = (SqlIndexOption) index.getIndexOption();
            if (indexOption.visiable != null) {
                isVisiable =
                        indexOption.visiable.getValue().equals(SqlMysqlConstraintEnable.VISIBLE);
            }

            if (indexOption.comment != null) {
                comment =
                        SqlNodeUtil.getSqlString(
                                indexOption.comment, SqlNodeUtil.EMPTY_SQL_DIALECT);
            }
        }
        return new IndexDefinition(
                indexType, index.getName().getSimple(), comment, columnInfos, isVisiable);
    }

    public static ConstraintDefinition parseConstraint(SqlTableConstraint constraint) {
        if (constraint instanceof SqlCheckConstraint) {
            throw new UnsupportedOperationException("not support convent SqlCheckConstraint ");
        }

        String name = null;
        if (constraint.indexName != null) {
            name = constraint.indexName.getSimple();
        } else if (constraint.name != null) {
            name = constraint.name.getSimple();
        }
        boolean isUnqiue =
                SqlConstraintSpec.UNIQUE.name().equalsIgnoreCase(constraint.uniqueSpec.toValue())
                        | SqlConstraintSpec.UNIQUE_KEY
                                .getDigest()
                                .equalsIgnoreCase(constraint.uniqueSpec.toValue())
                        | SqlConstraintSpec.UNIQUE_INDEX
                                .getDigest()
                                .equalsIgnoreCase(constraint.uniqueSpec.toValue());

        boolean isPrimary =
                SqlConstraintSpec.PRIMARY_KEY
                        .getDigest()
                        .equalsIgnoreCase(constraint.uniqueSpec.toValue());

        String comment = null;
        if (constraint.indexOption != null) {
            SqlIndexOption indexOption = (SqlIndexOption) constraint.indexOption;
            if (indexOption.comment != null) {
                comment =
                        SqlNodeUtil.getSqlString(
                                indexOption.comment, SqlNodeUtil.EMPTY_SQL_DIALECT);
            }
        }

        List<String> columns = new ArrayList<>();
        if (constraint.keyPartList != null
                && CollectionUtils.isNotEmpty(constraint.keyPartList.getList())) {
            for (SqlNode sqlNode : constraint.keyPartList.getList()) {
                columns.add(
                        ((KeyPart) sqlNode)
                                .getColName()
                                .toSqlString(SqlNodeUtil.EMPTY_SQL_DIALECT)
                                .getSql());
            }
        }

        if (constraint.referenceDefinition != null) {

            ReferenceDefinition referenceDefinition =
                    (ReferenceDefinition) constraint.referenceDefinition;

            List<String> referenceColumns = new ArrayList<>();
            if (referenceDefinition.keyPartList != null
                    && CollectionUtils.isNotEmpty(referenceDefinition.keyPartList.getList())) {
                for (SqlNode sqlNode : referenceDefinition.keyPartList.getList()) {
                    referenceColumns.add(
                            ((KeyPart) sqlNode)
                                    .getColName()
                                    .toSqlString(SqlNodeUtil.EMPTY_SQL_DIALECT)
                                    .getSql());
                }
            }

            ForeignKeyDefinition.Constraint constraint1 = null;
            if (ReferenceOptionEnums.RESTRICT.equals(
                    referenceDefinition.referenceOption.getValue())) {
                constraint1 = ForeignKeyDefinition.Constraint.RESTRICT;
            } else if (ReferenceOptionEnums.CASCADE.equals(
                    referenceDefinition.referenceOption.getValue())) {
                constraint1 = ForeignKeyDefinition.Constraint.CASCADE;
            } else if (ReferenceOptionEnums.SET_NULL.equals(
                    referenceDefinition.referenceOption.getValue())) {
                constraint1 = ForeignKeyDefinition.Constraint.SET_NULL;
            } else if (ReferenceOptionEnums.SET_DEFAULT.equals(
                    referenceDefinition.referenceOption.getValue())) {
                constraint1 = ForeignKeyDefinition.Constraint.SET_DEFAULT;
            } else if (ReferenceOptionEnums.NO_ACTION.equals(
                    referenceDefinition.referenceOption.getValue())) {
                constraint1 = ForeignKeyDefinition.Constraint.NO_ACTION;
            }
            if (referenceDefinition
                    .referenceSituation
                    .getValue()
                    .equals(ReferenceSituationEnum.ON_DELETE)) {
                return new ForeignKeyDefinition(
                        name,
                        columns,
                        comment,
                        new TableIdentifier(null, null, referenceDefinition.tableName.getSimple()),
                        referenceColumns,
                        constraint1,
                        null);
            } else {
                return new ForeignKeyDefinition(
                        name,
                        columns,
                        comment,
                        new TableIdentifier(null, null, referenceDefinition.tableName.getSimple()),
                        referenceColumns,
                        null,
                        constraint1);
            }

        } else {
            return new ConstraintDefinition(
                    name, isPrimary, isUnqiue, false, columns, null, comment);
        }
    }

    public static DdlOperator parseSqlAlterTableDrop(SqlAlterTableDrop drop) {
        DdlOperator ddlOperator;
        String name = null != drop.name ? drop.name.getSimple() : null;
        switch ((AlterTableTargetTypeEnum) drop.dropType.getValue()) {
            case CHECK:
            case CONSTRAINT:
            case PRIMARY_kEY:
                ddlOperator =
                        new ConstraintOperator(
                                EventType.DROP_CONSTRAINT,
                                drop.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                        drop.tableIdentifier),
                                new ConstraintDefinition(
                                        name,
                                        drop.dropType.getValue().equals(PRIMARY_kEY),
                                        false,
                                        drop.dropType.getValue().equals(CHECK),
                                        null,
                                        null,
                                        null));
                break;
            case FOREIGN_kEY:
                ddlOperator =
                        new ConstraintOperator(
                                EventType.DROP_CONSTRAINT,
                                drop.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                        drop.tableIdentifier),
                                new ForeignKeyDefinition(name, null, null, null, null, null, null));
                break;
            case INDEX:
            case KEY:
                ddlOperator =
                        new IndexOperator(
                                EventType.DROP_INDEX,
                                drop.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                        drop.tableIdentifier),
                                new IndexDefinition(null, name, null, null, null),
                                null);
                break;
            case COLUMN:
                ddlOperator =
                        new ColumnOperator(
                                EventType.DROP_COLUMN,
                                drop.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                        drop.tableIdentifier),
                                Collections.singletonList(
                                        new ColumnDefinition(
                                                name, null, null, null, null, null, null, null,
                                                null, null, null, null)),
                                false,
                                false,
                                null);
                break;
            default:
                throw new UnsupportedOperationException(
                        "not support type: " + drop.dropType.getValue());
        }
        return ddlOperator;
    }

    public static ColumnOperator parseSqlAlterTableAlterColumn(
            SqlAlterTableAlterColumn sqlAlterTableAlterColumn) {

        switch ((AlterTableColumnOperatorTypeEnum) sqlAlterTableAlterColumn.operator.getValue()) {
            case SET_DEFAULT:
                return new ColumnOperator(
                        EventType.ALTER_COLUMN,
                        sqlAlterTableAlterColumn.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                        SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                sqlAlterTableAlterColumn.tableIdentifier),
                        Collections.singletonList(
                                new ColumnDefinition(
                                        sqlAlterTableAlterColumn.symbol.getSimple(),
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        sqlAlterTableAlterColumn.value.toString(),
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        false,
                        true,
                        null);
            case DROP_DEFAULT:
                return new ColumnOperator(
                        EventType.ALTER_COLUMN,
                        sqlAlterTableAlterColumn.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                        SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                sqlAlterTableAlterColumn.tableIdentifier),
                        Collections.singletonList(
                                new ColumnDefinition(
                                        sqlAlterTableAlterColumn.symbol.getSimple(),
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        true,
                        false,
                        null);
            default:
                throw new RuntimeException(
                        "not support parse {} of SqlAlterTableAlterColumn"
                                + sqlAlterTableAlterColumn.operator.getValue());
        }
    }

    public static List<ColumnOperator> parseSqlAlterTableChangeColumn(
            SqlAlterTableChangeColumn sqlAlterTableChangeColumn,
            ColumnTypeConvert columnTypeConvert) {
        SqlGeneralColumn column = (SqlGeneralColumn) sqlAlterTableChangeColumn.column;

        // modify column操作
        if (sqlAlterTableChangeColumn.oldColumnName == null) {
            return Lists.newArrayList(
                    new ColumnOperator(
                            EventType.ALTER_COLUMN,
                            sqlAlterTableChangeColumn.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                            SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                    sqlAlterTableChangeColumn.tableIdentifier),
                            Collections.singletonList(parseColumn(column, columnTypeConvert)),
                            false,
                            false,
                            null));
        } else {
            // change column操作
            return Lists.newArrayList(
                    new ColumnOperator(
                            EventType.RENAME_COLUMN,
                            sqlAlterTableChangeColumn.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                            SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                    sqlAlterTableChangeColumn.tableIdentifier),
                            Collections.singletonList(
                                    new ColumnDefinition(
                                            sqlAlterTableChangeColumn.oldColumnName.getSimple(),
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null)),
                            false,
                            false,
                            column.name.getSimple()),
                    new ColumnOperator(
                            EventType.ALTER_COLUMN,
                            sqlAlterTableChangeColumn.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),
                            SqlNodeUtil.convertSqlIdentifierToTableIdentifier(
                                    sqlAlterTableChangeColumn.tableIdentifier),
                            Collections.singletonList(parseColumn(column, columnTypeConvert)),
                            false,
                            false,
                            null));
        }
    }

    public static String parseColumnDefinitionToString(
            ColumnDefinition columnDefinition,
            SqlDialect sqlDialect,
            ColumnTypeConvert columnTypeConvert) {
        StringBuilder sb = new StringBuilder(256);
        String typeName =
                columnTypeConvert.conventColumnTypeToDatSourceType(columnDefinition.getType());
        sb.append(sqlDialect.quoteIdentifier(columnDefinition.getName()))
                .append(" ")
                .append(typeName);
        ColumnTypeDesc columnTypeDesc =
                columnTypeConvert.getColumnTypeDesc(columnDefinition.getType());
        Integer precision = columnDefinition.getPrecision();
        if (precision != null) {
            if (columnTypeDesc != null) {
                precision = columnTypeDesc.getPrecision(precision);
            }
            if (precision != null) {
                sb.append("(").append(precision);
                Integer scale = columnDefinition.getScale();
                if (scale != null) {
                    if (columnTypeDesc != null) {
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
        sb.append(" ");
        if (columnDefinition.isNullable() != null && !columnDefinition.isNullable()) {
            sb.append("NOT NULL").append(" ");
        }
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

        if (columnDefinition.getAutoIncrement() != null && columnDefinition.getAutoIncrement()) {
            sb.append("auto_increment").append(" ");
        }

        if (columnDefinition.isPrimary() != null && columnDefinition.isPrimary()) {
            sb.append("PRIMARY KEY").append(" ");
        } else if (columnDefinition.isUnique() != null && columnDefinition.isUnique()) {
            sb.append("UNIQUE KEY").append(" ");
        }

        if (StringUtils.isNotBlank(columnDefinition.getCheck())) {
            sb.append("check").append(" (").append(columnDefinition.getCheck()).append(") ");
        }

        if (columnDefinition.getComment() != null) {
            sb.append("COMMENT ")
                    .append("'")
                    .append(columnDefinition.getComment())
                    .append("'")
                    .append(" ");
        }
        return sb.toString();
    }

    public static String parseIndexDefinitionToString(
            IndexDefinition indexDefinition, SqlDialect sqlDialect) {
        StringBuilder sb = new StringBuilder(256);
        if (indexDefinition.getIndexType() != null) {
            switch (indexDefinition.getIndexType()) {
                case FULLTEXT:
                    sb.append("FULLTEXT").append(" ");
                case UNIQUE:
                    sb.append("UNIQUE").append(" ");
                case SPATIAL:
                    sb.append("SPATIAL").append(" ");
            }
        }

        sb.append("INDEX").append(" ");

        if (indexDefinition.getIndexName() != null) {
            sb.append(sqlDialect.quoteIdentifier(indexDefinition.getIndexName())).append(" ");
        }

        if (CollectionUtils.isNotEmpty(indexDefinition.getColumns())) {
            sb.append("(").append(" ");
            sb.append(
                    indexDefinition.getColumns().stream()
                            .map(
                                    i -> {
                                        String name = i.getName();
                                        if (i.getLength() != null) {
                                            name = name + "(" + i.getLength() + ")";
                                            return name;
                                        }
                                        return sqlDialect.quoteIdentifier(name);
                                    })
                            .collect(Collectors.joining(",")));
            sb.append(")").append(" ");
        }

        if (indexDefinition.getComment() != null) {
            sb.append("COMMENT")
                    .append(" ")
                    .append("'")
                    .append(indexDefinition.getComment())
                    .append("'");
        }
        return sb.toString();
    }

    public static String parseConstraintDefinitionToString(
            ConstraintDefinition constraintDefinition, SqlDialect sqlDialect) {
        StringBuilder sb = new StringBuilder(256);
        if (constraintDefinition instanceof ForeignKeyDefinition) {
            sb.append("FOREIGN KEY").append(" ");
            if (StringUtils.isNotBlank(constraintDefinition.getName())) {
                sb.append(sqlDialect.quoteIdentifier(constraintDefinition.getName())).append(" ");
            }
            sb.append("(").append(" ");
            sb.append(
                    constraintDefinition.getColumns().stream()
                            .map(sqlDialect::quoteIdentifier)
                            .collect(Collectors.joining(",")));
            sb.append(")").append(" ");
            ForeignKeyDefinition foreignKeyDefinition = (ForeignKeyDefinition) constraintDefinition;
            sb.append("REFERENCES")
                    .append(" ")
                    .append(foreignKeyDefinition.getReferenceTable().getTable())
                    .append(" ");
            sb.append(String.join(",", foreignKeyDefinition.getReferenceColumns())).append(" ");
            if (foreignKeyDefinition.getOnDelete() != null) {
                if (foreignKeyConstraintTotMysqlString(foreignKeyDefinition.getOnDelete())
                        != null) {
                    sb.append("ON DELETE")
                            .append(" ")
                            .append(
                                    foreignKeyConstraintTotMysqlString(
                                            foreignKeyDefinition.getOnDelete()));
                }

            } else {
                if (foreignKeyConstraintTotMysqlString(foreignKeyDefinition.getOnUpdate())
                        != null) {
                    sb.append("ON UPDATE")
                            .append(" ")
                            .append(
                                    foreignKeyConstraintTotMysqlString(
                                            foreignKeyDefinition.getOnUpdate()));
                }
            }
            return sb.toString();
        }

        if (StringUtils.isNotBlank(constraintDefinition.getCheck())) {
            sb.append("CONSTRAINT").append(" ");
            if (StringUtils.isNotBlank(constraintDefinition.getName())) {
                sb.append(sqlDialect.quoteIdentifier(constraintDefinition.getName())).append(" ");
            }
            sb.append("CHECK").append(" ").append("(");
            sb.append(constraintDefinition.getCheck()).append(" ").append(")");
            return sb.toString();
        }

        if (constraintDefinition.isPrimary()) {
            sb.append("PRIMARY KEY").append(" ");
        } else if (constraintDefinition.isUnique()) {
            sb.append("UNIQUE INDEX").append(" ");
        }

        if (StringUtils.isNotBlank(constraintDefinition.getName())) {
            sb.append(sqlDialect.quoteIdentifier(constraintDefinition.getName())).append(" ");
        }
        sb.append("(").append(" ");
        sb.append(
                constraintDefinition.getColumns().stream()
                        .map(sqlDialect::quoteIdentifier)
                        .collect(Collectors.joining(",")));
        sb.append(")").append(" ");

        if (constraintDefinition.getComment() != null) {
            sb.append("COMMENT")
                    .append(" ")
                    .append("'")
                    .append(constraintDefinition.getComment())
                    .append("'");
        }

        return sb.toString();
    }

    public static String parseCreateDefinition(
            List<ColumnDefinition> columnList,
            List<IndexDefinition> indexList,
            List<ConstraintDefinition> constraintList,
            SqlDialect sqlDialect,
            ColumnTypeConvert columnTypeConvert) {
        if (CollectionUtils.isEmpty(columnList)
                && CollectionUtils.isEmpty(indexList)
                && CollectionUtils.isEmpty(constraintList)) {
            return "";
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

        if (CollectionUtils.isNotEmpty(indexList)) {
            indexList.forEach(i -> strings.add(parseIndexDefinitionToString(i, sqlDialect)));
        }

        if (CollectionUtils.isNotEmpty(constraintList)) {
            constraintList.forEach(
                    i -> strings.add(parseConstraintDefinitionToString(i, sqlDialect)));
        }
        sb.append(String.join(",", strings));
        sb.append(")");
        return sb.toString();
    }

    private static String foreignKeyConstraintTotMysqlString(
            ForeignKeyDefinition.Constraint constraint) {
        String s = null;
        switch (constraint) {
            case CASCADE:
                s = "CASCADE";
                break;
            case RESTRICT:
                s = "RESTRICT";
                break;
            case SET_NULL:
                s = "SET NULL";
                break;
            case NO_ACTION:
                s = "NO ACTION";
                break;
            case SET_DEFAULT:
                s = "SET DEFAULT";
                break;
        }
        return s;
    }

    public static SqlIdentifier replaceTableIdentifier(
            SqlIdentifier sqlIdentifier, MappingRule nameMapping) {
        TableIdentifier tableIdentifier =
                SqlNodeUtil.convertSqlIdentifierToTableIdentifier(sqlIdentifier);
        TableIdentifier tableIdentifierConvent =
                nameMapping.tableIdentifierMapping(tableIdentifier);
        tableIdentifierConvent = nameMapping.casingTableIdentifier(tableIdentifierConvent);
        return SqlNodeUtil.convertTableIdentifierToSqlIdentifier(tableIdentifierConvent);
    }

    public static SqlIdentifier replaceTableIdentifier(
            TableIdentifier tableIdentifier, MappingRule nameMapping) {
        TableIdentifier tableIdentifierConvent =
                nameMapping.tableIdentifierMapping(tableIdentifier);
        return SqlNodeUtil.convertTableIdentifierToSqlIdentifier(tableIdentifierConvent);
    }

    public static DataSourceFunction dataSourceFunction(String functionName) {
        return DataSourceFunction.valueOf(functionName);
    }

    public static String dataSourceFunctionString(DataSourceFunction dataSourceFunction) {
        return dataSourceFunction.name();
    }
}
