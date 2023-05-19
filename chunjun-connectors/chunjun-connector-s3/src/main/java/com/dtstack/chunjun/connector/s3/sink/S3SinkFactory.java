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

package com.dtstack.chunjun.connector.s3.sink;

import com.dtstack.chunjun.config.SpeedConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.s3.config.S3Config;
import com.dtstack.chunjun.connector.s3.converter.S3RawTypeMapper;
import com.dtstack.chunjun.connector.s3.converter.S3SqlConverter;
import com.dtstack.chunjun.connector.s3.converter.S3SyncConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class S3SinkFactory extends SinkFactory {

    private final S3Config s3Config;
    private final SpeedConfig speedConfig;

    public S3SinkFactory(SyncConfig syncConfig) {
        super(syncConfig);
        s3Config =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(syncConfig.getWriter().getParameter()),
                        S3Config.class);
        s3Config.setColumn(syncConfig.getWriter().getFieldList());
        speedConfig = syncConfig.getSpeed();
        super.initCommonConf(s3Config);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return S3RawTypeMapper::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        S3OutputFormatBuilder builder = new S3OutputFormatBuilder(new S3OutputFormat());
        final RowType rowType = TableUtil.createRowType(s3Config.getColumn(), getRawTypeMapper());
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter = new S3SyncConverter(rowType, s3Config);
        } else {
            rowConverter = new S3SqlConverter(rowType, s3Config);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        builder.setSpeedConf(speedConfig);
        builder.setS3Conf(s3Config);
        return createOutput(dataSet, builder.finish());
    }
}
