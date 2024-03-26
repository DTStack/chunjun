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

import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.connector.hbase.converter.HBaseSqlConverter;
import com.dtstack.chunjun.connector.hbase.source.HBaseInputFormatBuilder;
import com.dtstack.chunjun.connector.hbase.table.lookup.HBaseAllTableFunction;
import com.dtstack.chunjun.connector.hbase.table.lookup.HBaseLruTableFunction;
import com.dtstack.chunjun.connector.hbase.util.ScanBuilder;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;

import static com.dtstack.chunjun.connector.hbase.config.HBaseConfigConstants.MULTI_VERSION_FIXED_COLUMN;

public class HBaseDynamicTableSource extends BaseHBaseDynamicTableSource {

    private final HBaseConfig hBaseConfig;
    private final TableSchema tableSchema;
    private final LookupConfig lookupConfig;
    private HBaseTableSchema hbaseSchema;

    public HBaseDynamicTableSource(
            HBaseConfig conf,
            TableSchema tableSchema,
            LookupConfig lookupConfig,
            HBaseTableSchema hbaseSchema) {
        super(tableSchema, hbaseSchema, conf, lookupConfig);
        this.hBaseConfig = conf;
        this.tableSchema = tableSchema;
        this.lookupConfig = lookupConfig;
        this.hbaseSchema = hbaseSchema;
        this.hbaseSchema.setTableName(hBaseConfig.getTable());
    }

    @Override
    public DynamicTableSource copy() {
        return new HBaseDynamicTableSource(
                this.hBaseConfig, tableSchema, lookupConfig, hbaseSchema);
    }

    @Override
    public String asSummaryString() {
        return "Hbase2DynamicTableSource:";
    }

    @Override
    protected BaseRichInputFormatBuilder<?> getBaseRichInputFormatBuilder() {
        ScanBuilder scanBuilder = ScanBuilder.forSql(hbaseSchema);
        HBaseInputFormatBuilder builder =
                HBaseInputFormatBuilder.newBuild(hBaseConfig.getTable(), scanBuilder);
        builder.setColumnMetaInfos(hBaseConfig.getColumnMetaInfos());
        builder.setConfig(hBaseConfig);
        builder.setHbaseConfig(hBaseConfig.getHbaseConfig());
        builder.setStartRowKey(hBaseConfig.getStartRowkey());
        builder.setEndRowKey(hBaseConfig.getEndRowkey());
        builder.setIsBinaryRowkey(hBaseConfig.isBinaryRowkey());
        builder.setScanCacheSize(hBaseConfig.getScanCacheSize());
        builder.setScanBatchSize(hBaseConfig.getScanBatchSize());
        builder.setMode(hBaseConfig.getMode());
        builder.setMaxVersion(hBaseConfig.getMaxVersion());
        // 投影下推后, hbaseSchema 会被过滤无用的字段，而 tableSchema 不变, 后面根据 hbaseSchema 生成 hbase scan
        AbstractRowConverter rowConverter = new HBaseSqlConverter(hbaseSchema, hBaseConfig);
        if (hBaseConfig.getMode().equalsIgnoreCase(MULTI_VERSION_FIXED_COLUMN)) {
            rowConverter =
                    new HBaseSqlConverter(
                            InternalTypeInfo.of(
                                            tableSchema.toPhysicalRowDataType().getLogicalType())
                                    .toRowType(),
                            hBaseConfig);
        }
        builder.setRowConverter(rowConverter);
        return builder;
    }

    @Override
    protected AbstractLruTableFunction getAbstractLruTableFunction() {
        AbstractRowConverter rowConverter = new HBaseSqlConverter(hbaseSchema, hBaseConfig);
        return new HBaseLruTableFunction(lookupConfig, hbaseSchema, hBaseConfig, rowConverter);
    }

    @Override
    protected AbstractAllTableFunction getAbstractAllTableFunction() {
        return new HBaseAllTableFunction(lookupConfig, hbaseSchema, hBaseConfig);
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.hbaseSchema =
                HBaseTableSchema.fromDataType(
                        Projection.of(projectedFields).project(hbaseSchema.convertToDataType()),
                        hBaseConfig);
    }
}
