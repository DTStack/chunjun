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

import com.dtstack.chunjun.conf.FieldConfig;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.lookup.JdbcAllTableFunction;
import com.dtstack.chunjun.connector.jdbc.lookup.JdbcLruTableFunction;
import com.dtstack.chunjun.connector.jdbc.util.key.KeyUtil;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.conf.LookupConf;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelTableFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A {@link DynamicTableSource} for JDBC. */
public class JdbcDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    protected final JdbcConfig jdbcConf;
    protected final LookupConf lookupConf;
    protected final String dialectName;
    protected final JdbcDialect jdbcDialect;
    protected final JdbcInputFormatBuilder builder;
    protected TableSchema physicalSchema;

    public JdbcDynamicTableSource(
            JdbcConfig jdbcConf,
            LookupConf lookupConf,
            TableSchema physicalSchema,
            JdbcDialect jdbcDialect,
            JdbcInputFormatBuilder builder) {
        this.jdbcConf = jdbcConf;
        this.lookupConf = lookupConf;
        this.physicalSchema = physicalSchema;
        this.jdbcDialect = jdbcDialect;
        this.dialectName = jdbcDialect.dialectName();
        this.builder = builder;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }
        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        if (lookupConf.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelTableFunctionProvider.of(
                    new JdbcAllTableFunction(
                            jdbcConf,
                            jdbcDialect,
                            lookupConf,
                            physicalSchema.getFieldNames(),
                            keyNames,
                            rowType),
                    lookupConf.getParallelism());
        }
        return ParallelAsyncTableFunctionProvider.of(
                new JdbcLruTableFunction(
                        jdbcConf,
                        jdbcDialect,
                        lookupConf,
                        physicalSchema.getFieldNames(),
                        keyNames,
                        rowType),
                lookupConf.getParallelism());
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        JdbcInputFormatBuilder builder = this.builder;
        String[] fieldNames = physicalSchema.getFieldNames();
        List<FieldConfig> columnList = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            FieldConfig field = new FieldConfig();
            field.setName(fieldNames[i]);
            field.setType(rowType.getTypeAt(i).asSummaryString());
            field.setIndex(i);
            columnList.add(field);
        }
        jdbcConf.setColumn(columnList);

        // TODO sql任务使用增量同步或者间隔轮询时暂不支持增量指标写入外部存储，暂时设置为false
        jdbcConf.setInitReporter(false);

        KeyUtil<?, BigInteger> restoreKeyUtil = null;
        KeyUtil<?, BigInteger> splitKeyUtil = null;
        KeyUtil<?, BigInteger> incrementKeyUtil = null;

        // init restore info
        String restoreColumn = jdbcConf.getRestoreColumn();
        if (StringUtils.isNotBlank(restoreColumn)) {
            FieldConfig fieldConfig =
                    FieldConfig.getSameNameMetaColumn(jdbcConf.getColumn(), restoreColumn);
            if (fieldConfig != null) {
                jdbcConf.setRestoreColumnIndex(fieldConfig.getIndex());
                jdbcConf.setRestoreColumnType(fieldConfig.getType());
                restoreKeyUtil =
                        jdbcDialect.initKeyUtil(fieldConfig.getName(), fieldConfig.getType());
            } else {
                throw new IllegalArgumentException("unknown restore column name: " + restoreColumn);
            }
        }

        // init splitInfo
        String splitPk = jdbcConf.getSplitPk();
        if (StringUtils.isNotBlank(splitPk)) {
            FieldConfig fieldConfig =
                    FieldConfig.getSameNameMetaColumn(jdbcConf.getColumn(), splitPk);
            if (fieldConfig != null) {
                jdbcConf.setSplitPk(fieldConfig.getType());
                splitKeyUtil =
                        jdbcDialect.initKeyUtil(fieldConfig.getName(), fieldConfig.getType());
            }
        }

        // init incrementInfo
        String incrementColumn = jdbcConf.getIncreColumn();
        if (StringUtils.isNotBlank(incrementColumn)) {
            FieldConfig fieldConfig =
                    FieldConfig.getSameNameMetaColumn(jdbcConf.getColumn(), incrementColumn);
            int index;
            String name;
            String type;
            if (fieldConfig != null) {
                index = fieldConfig.getIndex();
                name = fieldConfig.getName();
                type = fieldConfig.getType();
                incrementKeyUtil = jdbcDialect.initKeyUtil(name, type);
            } else {
                throw new IllegalArgumentException(
                        "unknown increment column name: " + incrementColumn);
            }
            jdbcConf.setIncreColumn(name);
            jdbcConf.setIncreColumnType(type);
            jdbcConf.setIncreColumnIndex(index);

            jdbcConf.setRestoreColumn(name);
            jdbcConf.setRestoreColumnIndex(index);
            jdbcConf.setRestoreColumnType(type);
            restoreKeyUtil = incrementKeyUtil;

            if (StringUtils.isBlank(jdbcConf.getSplitPk())) {
                splitKeyUtil = incrementKeyUtil;
                jdbcConf.setSplitPk(name);
            }
        }

        builder.setRestoreKeyUtil(restoreKeyUtil);
        builder.setSplitKeyUtil(splitKeyUtil);
        builder.setIncrementKeyUtil(incrementKeyUtil);

        builder.setColumnNameList(
                jdbcConf.getColumn().stream()
                        .map(FieldConfig::getName)
                        .collect(Collectors.toList()));

        builder.setJdbcDialect(jdbcDialect);
        builder.setJdbcConf(jdbcConf);
        builder.setRowConverter(jdbcDialect.getRowConverter(rowType));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                jdbcConf.getParallelism());
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
    public void applyProjection(int[][] projectedFields) {
        checkArgument(
                TableSchemaUtils.containsPhysicalColumnsOnly(physicalSchema),
                "Projection is only supported for physical columns.");
        TableSchema.Builder builder = TableSchema.builder();

        FieldsDataType fields =
                (FieldsDataType)
                        DataTypeUtils.projectRow(physicalSchema.toRowDataType(), projectedFields);
        RowType topFields = (RowType) fields.getLogicalType();
        for (int i = 0; i < topFields.getFieldCount(); i++) {
            builder.field(topFields.getFieldNames().get(i), fields.getChildren().get(i));
        }

        this.physicalSchema = builder.build();
    }

    @Override
    public DynamicTableSource copy() {
        return new JdbcDynamicTableSource(
                jdbcConf, lookupConf, physicalSchema, jdbcDialect, builder);
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
        return Objects.equals(jdbcConf, that.jdbcConf)
                && Objects.equals(lookupConf, that.lookupConf)
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jdbcConf, lookupConf, physicalSchema, dialectName);
    }
}
