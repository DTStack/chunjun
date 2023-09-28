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

package com.dtstack.chunjun.connector.iceberg.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.iceberg.config.IcebergConfig;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.sink.WriteMode;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergSinkFactory extends SinkFactory {

    private final IcebergConfig icebergConfig;

    public IcebergSinkFactory(SyncConfig config) {
        super(config);
        icebergConfig =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()),
                        IcebergConfig.class);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return null;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        List<String> columns =
                icebergConfig.getColumn().stream()
                        .map(FieldConfig::getName)
                        .collect(Collectors.toList());

        DataStream<RowData> convertedDataStream =
                dataSet.map(new ChunjunRowDataConvertMap(icebergConfig.getColumn()));

        boolean isOverwrite =
                icebergConfig.getWriteMode().equalsIgnoreCase(WriteMode.OVERWRITE.name());
        return FlinkSink.forRowData(convertedDataStream)
                .tableLoader(buildTableLoader())
                .writeParallelism(icebergConfig.getParallelism())
                .equalityFieldColumns(columns)
                .overwrite(isOverwrite)
                .build();
    }

    private TableLoader buildTableLoader() {
        Map<String, String> icebergProps = new HashMap<>();
        icebergProps.put("warehouse", icebergConfig.getWarehouse());
        icebergProps.put("uri", icebergConfig.getUri());

        /* build hadoop configuration */
        Configuration configuration = new Configuration();
        icebergConfig
                .getHadoopConfig()
                .forEach((key, value) -> configuration.set(key, (String) value));

        CatalogLoader hc =
                CatalogLoader.hive(icebergConfig.getDatabase(), configuration, icebergProps);
        TableLoader tl =
                TableLoader.fromCatalog(
                        hc,
                        TableIdentifier.of(icebergConfig.getDatabase(), icebergConfig.getTable()));

        if (tl instanceof TableLoader.CatalogTableLoader) {
            tl.open();
        }

        return tl;
    }
}
