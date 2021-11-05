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
package com.dtstack.flinkx.connector.hbase14.table;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hbase.HBaseConverter;
import com.dtstack.flinkx.connector.hbase.HBaseTableSchema;
import com.dtstack.flinkx.connector.hbase14.conf.HBaseConf;
import com.dtstack.flinkx.connector.hbase14.source.HBaseInputFormatBuilder;
import com.dtstack.flinkx.connector.hbase14.table.lookup.HBaseAllTableFunction;
import com.dtstack.flinkx.connector.hbase14.table.lookup.HBaseLruTableFunction;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.enums.CacheType;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.source.DtInputFormatSourceFunction;
import com.dtstack.flinkx.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.flinkx.table.connector.source.ParallelSourceFunctionProvider;
import com.dtstack.flinkx.table.connector.source.ParallelTableFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2021/06/17 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HBaseDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    private final HBaseConf conf;
    private TableSchema tableSchema;
    private final LookupConf lookupConf;
    private final HBaseTableSchema hbaseSchema;

    public HBaseDynamicTableSource(
            HBaseConf conf,
            TableSchema tableSchema,
            LookupConf lookupConf,
            HBaseTableSchema hbaseSchema) {
        this.conf = conf;
        this.tableSchema = tableSchema;
        this.lookupConf = lookupConf;
        this.hbaseSchema = hbaseSchema;
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
        conf.setColumn(columnList);
        HBaseInputFormatBuilder builder = new HBaseInputFormatBuilder();
        builder.setColumnMetaInfos(conf.getColumnMetaInfos());
        builder.setConfig(conf);
        builder.setEncoding(conf.getEncoding());
        builder.setHbaseConfig(conf.getHbaseConfig());
        builder.setTableName(conf.getTable());
        AbstractRowConverter rowConverter = new HBaseConverter(rowType);
        builder.setRowConverter(rowConverter);
        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                true,
                conf.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new HBaseDynamicTableSource(this.conf, tableSchema, lookupConf, hbaseSchema);
    }

    @Override
    public String asSummaryString() {
        return "HdfsDynamicTableSource:";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "redis only support non-nested look up keys");
            keyNames[i] = tableSchema.getFieldNames()[innerKeyArr[0]];
        }
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

        if (lookupConf.getCache().equalsIgnoreCase(CacheType.LRU.toString())) {
            return ParallelAsyncTableFunctionProvider.of(
                    new HBaseLruTableFunction(conf, lookupConf, hbaseSchema),
                    lookupConf.getParallelism());
        }
        return ParallelTableFunctionProvider.of(
                new HBaseAllTableFunction(
                        conf, lookupConf, tableSchema.getFieldNames(), keyNames, hbaseSchema),
                lookupConf.getParallelism());
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.tableSchema = TableSchemaUtils.projectSchema(tableSchema, projectedFields);
    }
}
