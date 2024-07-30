/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.starrocks.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksRawTypeMapper;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksSqlConverter;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksSyncConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.stream.Collectors;

public class StarRocksSinkFactory extends SinkFactory {

    private final StarRocksConfig starRocksConfig;

    public StarRocksSinkFactory(SyncConfig syncConfig) {
        super(syncConfig);
        starRocksConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConfig.getWriter().getParameter()),
                        StarRocksConfig.class);

        int batchSize = (int) syncConfig.getWriter().getLongVal("batchSize", 1024);
        starRocksConfig.setBatchSize(batchSize);
        super.initCommonConf(starRocksConfig);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return StarRocksRawTypeMapper::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        StarRocksOutputFormatBuilder builder =
                new StarRocksOutputFormatBuilder(new StarRocksOutputFormat());
        builder.setStarRocksConf(starRocksConfig);
        RowType rowType = TableUtil.createRowType(starRocksConfig.getColumn(), getRawTypeMapper());
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter = new StarRocksSyncConverter(rowType, starRocksConfig);
        } else {
            rowConverter =
                    new StarRocksSqlConverter(
                            rowType,
                            starRocksConfig.getColumn().stream()
                                    .map(FieldConfig::getName)
                                    .collect(Collectors.toList()));
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createOutput(dataSet, builder.finish());
    }
}
