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

package com.dtstack.chunjun.connector.rabbitmq.source;

import com.dtstack.chunjun.connector.rabbitmq.options.RabbitmqOptions;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;

public class RabbitmqDynamicTableSource implements ScanTableSource {
    private final DecodingFormat<DeserializationSchema<RowData>> messageDecodingFormat;

    private final ResolvedSchema resolvedSchema;
    private final ReadableConfig config;

    private final RMQConnectionConfig rmqConnectionConfig;

    public RabbitmqDynamicTableSource(
            ResolvedSchema resolvedSchema,
            ReadableConfig config,
            RMQConnectionConfig rmqConnectionConfig,
            DecodingFormat<DeserializationSchema<RowData>> messageDecodingFormat) {
        this.resolvedSchema = resolvedSchema;
        this.config = config;
        this.rmqConnectionConfig = rmqConnectionConfig;
        this.messageDecodingFormat = messageDecodingFormat;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return messageDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final DeserializationSchema<RowData> messageDeserializationSchema =
                messageDecodingFormat.createRuntimeDecoder(
                        runtimeProviderContext, resolvedSchema.toPhysicalRowDataType());

        return ParallelSourceFunctionProvider.of(
                new RMQSource<>(
                        rmqConnectionConfig,
                        config.get(RabbitmqOptions.QUEUE_NAME),
                        config.get(RabbitmqOptions.USE_CORRELATION_ID),
                        messageDeserializationSchema),
                false,
                1);
    }

    @Override
    public DynamicTableSource copy() {
        return new RabbitmqDynamicTableSource(
                resolvedSchema, config, rmqConnectionConfig, messageDecodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "Rabbitmq table source";
    }
}
