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

package com.dtstack.chunjun.connector.hbase.table;

import com.dtstack.chunjun.config.FieldConf;
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.connector.hbase.util.HBaseConfigUtils;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConf;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelTableFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseHBaseDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {
    protected TableSchema tableSchema;
    protected HBaseTableSchema hbaseSchema;

    protected final HBaseConfig hBaseConfig;
    protected final LookupConf lookupConf;

    public BaseHBaseDynamicTableSource(
            TableSchema tableSchema,
            HBaseTableSchema hbaseSchema,
            HBaseConfig hBaseConfig,
            LookupConf lookupConf) {
        this.tableSchema = tableSchema;
        this.hbaseSchema = hbaseSchema;
        this.hBaseConfig = hBaseConfig;
        this.hbaseSchema.setTableName(hBaseConfig.getTable());
        this.lookupConf = lookupConf;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        String[] fieldNames = tableSchema.getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            FieldConf field = new FieldConf();
            field.setName(fieldNames[i]);
            field.setType(rowType.getTypeAt(i).asSummaryString());
            field.setIndex(i);
            columnList.add(field);
        }
        hBaseConfig.setColumn(columnList);

        BaseRichInputFormatBuilder<?> builder = getBaseRichInputFormatBuilder();

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                true,
                hBaseConfig.getParallelism());
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        if (HBaseConfigUtils.isEnableKerberos(hBaseConfig.getHbaseConfig())) {
            HBaseConfigUtils.fillKerberosConfig(hBaseConfig.getHbaseConfig());
        }
        hbaseSchema.setTableName(hBaseConfig.getTable());
        if (lookupConf.getCache().equalsIgnoreCase(CacheType.LRU.toString())) {
            return ParallelAsyncTableFunctionProvider.of(
                    getAbstractLruTableFunction(), lookupConf.getParallelism());
        }
        return ParallelTableFunctionProvider.of(
                getAbstractAllTableFunction(), lookupConf.getParallelism());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        TableSchema projectSchema =
                TableSchemaUtils.projectSchema(
                        hbaseSchema.convertsToTableSchema(), projectedFields);
        this.hbaseSchema = HBaseTableSchema.fromTableSchema(projectSchema);
    }

    protected abstract BaseRichInputFormatBuilder<?> getBaseRichInputFormatBuilder();

    protected abstract AbstractLruTableFunction getAbstractLruTableFunction();

    protected abstract AbstractAllTableFunction getAbstractAllTableFunction();
}
