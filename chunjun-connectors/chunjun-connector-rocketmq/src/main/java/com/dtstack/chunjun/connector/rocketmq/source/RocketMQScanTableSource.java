/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.rocketmq.source;

import com.dtstack.chunjun.connector.rocketmq.config.RocketMQConfig;
import com.dtstack.chunjun.connector.rocketmq.converter.RocketMQSqlConverter;
import com.dtstack.chunjun.connector.rocketmq.source.deserialization.KeyValueDeserializationSchema;
import com.dtstack.chunjun.connector.rocketmq.source.deserialization.RowKeyValueDeserializationSchema;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Defines the scan table source of RocketMQ. */
public class RocketMQScanTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema schema;
    private final RocketMQConfig rocketMQConfig;
    private AbstractRowConverter converter;
    private List<String> metadataKeys;
    private final Integer parallelism;

    public RocketMQScanTableSource(
            ResolvedSchema schema, RocketMQConfig rocketMQConfig, Integer parallelism) {
        this.schema = schema;
        this.rocketMQConfig = rocketMQConfig;
        this.parallelism = parallelism;
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        LogicalType logicalType = schema.toPhysicalRowDataType().getLogicalType();

        String[] fieldNames = schema.getColumnNames().toArray(new String[0]);
        converter =
                new RocketMQSqlConverter(
                        InternalTypeInfo.of(logicalType).toRowType(),
                        rocketMQConfig.getEncoding(),
                        fieldNames);

        return ParallelSourceFunctionProvider.of(
                new com.dtstack.chunjun.connector.rocketmq.source.RocketMQSourceFunction<>(
                        createKeyValueDeserializationSchema(), rocketMQConfig),
                isBounded(),
                parallelism);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Collections.emptyMap();
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
    }

    @Override
    public DynamicTableSource copy() {
        RocketMQScanTableSource tableSource =
                new RocketMQScanTableSource(schema, rocketMQConfig, parallelism);
        tableSource.metadataKeys = metadataKeys;
        return tableSource;
    }

    @Override
    public String asSummaryString() {
        return RocketMQScanTableSource.class.getName();
    }

    private boolean isBounded() {
        return rocketMQConfig.getEndTimeMs() != Long.MAX_VALUE;
    }

    private KeyValueDeserializationSchema<RowData> createKeyValueDeserializationSchema() {
        return new RowKeyValueDeserializationSchema.Builder()
                .setTableSchema(schema)
                .setConverter(converter)
                .build();
    }
}
