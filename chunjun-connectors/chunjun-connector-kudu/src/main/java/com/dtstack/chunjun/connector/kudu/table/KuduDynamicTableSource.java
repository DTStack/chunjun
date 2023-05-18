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

package com.dtstack.chunjun.connector.kudu.table;

import com.dtstack.chunjun.connector.kudu.config.KuduLookupConfig;
import com.dtstack.chunjun.connector.kudu.config.KuduSourceConfig;
import com.dtstack.chunjun.connector.kudu.converter.KuduSqlConverter;
import com.dtstack.chunjun.connector.kudu.source.KuduInputFormatBuilder;
import com.dtstack.chunjun.connector.kudu.table.lookup.KuduAllTableFunction;
import com.dtstack.chunjun.connector.kudu.table.lookup.KuduLruTableFunction;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.List;

public class KuduDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    private static final String IDENTIFIER = "Kudu";

    private final KuduSourceConfig sourceConf;

    private final KuduLookupConfig kuduLookupConfig;

    private final ResolvedSchema tableSchema;

    public KuduDynamicTableSource(
            KuduSourceConfig sourceConf,
            KuduLookupConfig lookupConfig,
            ResolvedSchema tableSchema) {
        this.sourceConf = sourceConf;
        this.tableSchema = tableSchema;
        this.kuduLookupConfig = lookupConfig;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        KuduInputFormatBuilder builder = new KuduInputFormatBuilder();

        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(rowType);

        builder.setKuduSourceConf(sourceConf);
        builder.setRowConverter(new KuduSqlConverter(rowType, tableSchema.getColumnNames()));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInfo),
                true,
                sourceConf.getParallelism());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public DynamicTableSource copy() {
        return new KuduDynamicTableSource(sourceConf, kuduLookupConfig, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return IDENTIFIER;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        List<String> columnNames = tableSchema.getColumnNames();
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "Kudu only support non-nested look up keys");
            keyNames[i] = columnNames.get(innerKeyArr[0]);
        }
        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();

        if (kuduLookupConfig.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelLookupFunctionProvider.of(
                    new KuduAllTableFunction(
                            kuduLookupConfig,
                            new KuduSqlConverter(rowType, columnNames),
                            columnNames.toArray(new String[0]),
                            keyNames),
                    kuduLookupConfig.getParallelism());
        }
        return ParallelAsyncLookupFunctionProvider.of(
                new KuduLruTableFunction(
                        kuduLookupConfig,
                        new KuduSqlConverter(rowType, columnNames),
                        columnNames.toArray(new String[0]),
                        keyNames),
                kuduLookupConfig.getParallelism());
    }

    @Override
    public boolean supportsNestedProjection() {
        // kudu doesn't support nested projection
        return false;
    }
}
