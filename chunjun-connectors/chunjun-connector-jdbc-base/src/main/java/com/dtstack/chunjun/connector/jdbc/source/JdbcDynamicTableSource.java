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

package com.dtstack.chunjun.connector.jdbc.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.lookup.JdbcAllTableFunction;
import com.dtstack.chunjun.connector.jdbc.lookup.JdbcLruTableFunction;
import com.dtstack.chunjun.connector.jdbc.util.key.KeyUtil;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** A {@link DynamicTableSource} for JDBC. */
public class JdbcDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    protected final JdbcConfig jdbcConfig;
    protected final LookupConfig lookupConfig;
    protected final String dialectName;
    protected final JdbcDialect jdbcDialect;
    protected final JdbcInputFormatBuilder builder;
    protected ResolvedSchema resolvedSchema;
    private DataType physicalRowDataType;

    public JdbcDynamicTableSource(
            JdbcConfig jdbcConfig,
            LookupConfig lookupConfig,
            ResolvedSchema resolvedSchema,
            JdbcDialect jdbcDialect,
            JdbcInputFormatBuilder builder) {
        this.jdbcConfig = jdbcConfig;
        this.lookupConfig = lookupConfig;
        this.resolvedSchema = resolvedSchema;
        this.jdbcDialect = jdbcDialect;
        this.dialectName = jdbcDialect.dialectName();
        this.builder = builder;
        this.physicalRowDataType = resolvedSchema.toPhysicalRowDataType();
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = resolvedSchema.getColumnNames().get(innerKeyArr[0]);
        }
        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType =
                InternalTypeInfo.of(resolvedSchema.toPhysicalRowDataType().getLogicalType())
                        .toRowType();

        if (lookupConfig.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelLookupFunctionProvider.of(
                    new JdbcAllTableFunction(
                            jdbcConfig,
                            jdbcDialect,
                            lookupConfig,
                            resolvedSchema.getColumnNames().toArray(new String[0]),
                            keyNames,
                            rowType),
                    lookupConfig.getParallelism());
        }
        return ParallelAsyncLookupFunctionProvider.of(
                new JdbcLruTableFunction(
                        jdbcConfig,
                        jdbcDialect,
                        lookupConfig,
                        resolvedSchema.getColumnNames().toArray(new String[0]),
                        keyNames,
                        rowType),
                lookupConfig.getParallelism());
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);
        ResolvedSchema projectionSchema =
                ResolvedSchema.physical(rowType.getFieldNames(), physicalRowDataType.getChildren());
        JdbcInputFormatBuilder builder = this.builder;
        List<Column> columns = projectionSchema.getColumns();
        List<FieldConfig> columnList = new ArrayList<>(columns.size());
        for (Column column : columns) {
            FieldConfig field = new FieldConfig();
            field.setName(column.getName());
            field.setType(
                    TypeConfig.fromString(column.getDataType().getLogicalType().asSummaryString()));
            field.setIndex(columns.indexOf(column));
            columnList.add(field);
        }
        jdbcConfig.setColumn(columnList);

        // TODO sql任务使用增量同步或者间隔轮询时暂不支持增量指标写入外部存储，暂时设置为false
        jdbcConfig.setInitReporter(false);

        KeyUtil<?, BigInteger> restoreKeyUtil = null;
        KeyUtil<?, BigInteger> splitKeyUtil = null;
        KeyUtil<?, BigInteger> incrementKeyUtil = null;

        // init restore info
        String restoreColumn = jdbcConfig.getRestoreColumn();
        if (StringUtils.isNotBlank(restoreColumn)) {
            FieldConfig fieldConfig =
                    FieldConfig.getSameNameMetaColumn(jdbcConfig.getColumn(), restoreColumn);
            if (fieldConfig != null) {
                jdbcConfig.setRestoreColumnIndex(fieldConfig.getIndex());
                jdbcConfig.setRestoreColumnType(fieldConfig.getType().getType());
                restoreKeyUtil =
                        jdbcDialect.initKeyUtil(fieldConfig.getName(), fieldConfig.getType());
            } else {
                throw new IllegalArgumentException("unknown restore column name: " + restoreColumn);
            }
        }

        // init splitInfo
        String splitPk = jdbcConfig.getSplitPk();
        if (StringUtils.isNotBlank(splitPk)) {
            FieldConfig fieldConfig =
                    FieldConfig.getSameNameMetaColumn(jdbcConfig.getColumn(), splitPk);
            if (fieldConfig != null) {
                jdbcConfig.setSplitPk(fieldConfig.getName());
                splitKeyUtil =
                        jdbcDialect.initKeyUtil(fieldConfig.getName(), fieldConfig.getType());
            }
        }

        // init incrementInfo
        String incrementColumn = jdbcConfig.getIncreColumn();
        if (StringUtils.isNotBlank(incrementColumn)) {
            FieldConfig fieldConfig =
                    FieldConfig.getSameNameMetaColumn(jdbcConfig.getColumn(), incrementColumn);
            int index;
            String name;
            TypeConfig type;
            if (fieldConfig != null) {
                index = fieldConfig.getIndex();
                name = fieldConfig.getName();
                type = fieldConfig.getType();
                incrementKeyUtil = jdbcDialect.initKeyUtil(name, type);
            } else {
                throw new IllegalArgumentException(
                        "unknown increment column name: " + incrementColumn);
            }
            jdbcConfig.setIncreColumn(name);
            jdbcConfig.setIncreColumnType(type.getType());
            jdbcConfig.setIncreColumnIndex(index);

            jdbcConfig.setRestoreColumn(name);
            jdbcConfig.setRestoreColumnIndex(index);
            jdbcConfig.setRestoreColumnType(type.getType());
            restoreKeyUtil = incrementKeyUtil;

            if (StringUtils.isBlank(jdbcConfig.getSplitPk())) {
                splitKeyUtil = incrementKeyUtil;
                jdbcConfig.setSplitPk(name);
            }
        }

        builder.setRestoreKeyUtil(restoreKeyUtil);
        builder.setSplitKeyUtil(splitKeyUtil);
        builder.setIncrementKeyUtil(incrementKeyUtil);

        builder.setColumnNameList(
                jdbcConfig.getColumn().stream()
                        .map(FieldConfig::getName)
                        .collect(Collectors.toList()));

        builder.setJdbcDialect(jdbcDialect);
        builder.setJdbcConf(jdbcConfig);
        builder.setRowConverter(jdbcDialect.getRowConverter(rowType));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                jdbcConfig.getParallelism());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        // JDBC doesn't support nested projection
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }

    @Override
    public DynamicTableSource copy() {
        return new JdbcDynamicTableSource(
                jdbcConfig, lookupConfig, resolvedSchema, jdbcDialect, builder);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcDynamicTableSource)) {
            return false;
        }
        JdbcDynamicTableSource that = (JdbcDynamicTableSource) o;
        return Objects.equals(jdbcConfig, that.jdbcConfig)
                && Objects.equals(lookupConfig, that.lookupConfig)
                && Objects.equals(resolvedSchema, that.resolvedSchema)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jdbcConfig, lookupConfig, resolvedSchema, dialectName);
    }
}
