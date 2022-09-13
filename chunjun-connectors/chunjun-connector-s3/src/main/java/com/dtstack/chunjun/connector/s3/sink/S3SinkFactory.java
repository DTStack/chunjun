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

import com.dtstack.chunjun.conf.SpeedConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.s3.conf.S3Conf;
import com.dtstack.chunjun.connector.s3.converter.S3ColumnConverter;
import com.dtstack.chunjun.connector.s3.converter.S3RawConverter;
import com.dtstack.chunjun.connector.s3.converter.S3RowConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class S3SinkFactory extends SinkFactory {

    private final S3Conf s3Conf;
    private final SpeedConf speedConf;

    public S3SinkFactory(SyncConf syncConf) {
        super(syncConf);
        s3Conf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(syncConf.getWriter().getParameter()), S3Conf.class);
        s3Conf.setColumn(syncConf.getWriter().getFieldList());
        speedConf = syncConf.getSpeed();
        super.initCommonConf(s3Conf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return S3RawConverter::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        S3OutputFormatBuilder builder = new S3OutputFormatBuilder(new S3OutputFormat());
        final RowType rowType = TableUtil.createRowType(s3Conf.getColumn(), getRawTypeConverter());
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter = new S3ColumnConverter(rowType, s3Conf);
        } else {
            rowConverter = new S3RowConverter(rowType, s3Conf);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        builder.setSpeedConf(speedConf);
        builder.setS3Conf(s3Conf);
        return createOutput(dataSet, builder.finish());
    }
}
