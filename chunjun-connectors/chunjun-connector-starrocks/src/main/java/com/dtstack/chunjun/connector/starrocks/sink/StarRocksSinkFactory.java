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

import com.dtstack.chunjun.config.FieldConf;
import com.dtstack.chunjun.config.SyncConf;
import com.dtstack.chunjun.connector.starrocks.conf.StarRocksConf;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksColumnConverter;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksRawTypeConverter;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksRowConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.stream.Collectors;

/**
 * @author lihongwei
 * @date 2022/04/11
 */
public class StarRocksSinkFactory extends SinkFactory {

    private final StarRocksConf starRocksConf;

    public StarRocksSinkFactory(SyncConf syncConf) {
        super(syncConf);
        starRocksConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()), StarRocksConf.class);

        int batchSize = syncConf.getWriter().getIntVal("batchSize", 10240);
        starRocksConf.setBatchSize(batchSize);
        super.initCommonConf(starRocksConf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return StarRocksRawTypeConverter::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        StarRocksOutputFormatBuilder builder =
                new StarRocksOutputFormatBuilder(new StarRocksOutputFormat());
        builder.setStarRocksConf(starRocksConf);
        RowType rowType = TableUtil.createRowType(starRocksConf.getColumn(), getRawTypeConverter());
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter = new StarRocksColumnConverter(rowType, starRocksConf);
        } else {
            rowConverter =
                    new StarRocksRowConverter(
                            rowType,
                            starRocksConf.getColumn().stream()
                                    .map(FieldConf::getName)
                                    .collect(Collectors.toList()));
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createOutput(dataSet, builder.finish());
    }
}
