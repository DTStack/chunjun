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

package com.dtstack.chunjun.connector.s3.source;

import com.dtstack.chunjun.config.RestoreConf;
import com.dtstack.chunjun.config.SpeedConf;
import com.dtstack.chunjun.config.SyncConf;
import com.dtstack.chunjun.connector.s3.conf.S3Conf;
import com.dtstack.chunjun.connector.s3.converter.S3ColumnConverter;
import com.dtstack.chunjun.connector.s3.converter.S3RawConverter;
import com.dtstack.chunjun.connector.s3.converter.S3RowConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3SourceFactory extends SourceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(S3SourceFactory.class);

    private final S3Conf s3Conf;
    private final RestoreConf restoreConf;
    private final SpeedConf speedConf;

    public S3SourceFactory(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        s3Conf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getReader().getParameter()), S3Conf.class);
        s3Conf.setColumn(config.getReader().getFieldList());
        restoreConf = config.getRestore();
        speedConf = config.getSpeed();
        super.initCommonConf(s3Conf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return S3RawConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        S3InputFormatBuilder builder = new S3InputFormatBuilder(new S3InputFormat());
        builder.setRestoreConf(restoreConf);
        builder.setSpeedConf(speedConf);
        builder.setS3Conf(s3Conf);

        AbstractRowConverter rowConverter;
        final RowType rowType = TableUtil.createRowType(s3Conf.getColumn(), getRawTypeConverter());
        if (useAbstractBaseColumn) {
            rowConverter = new S3ColumnConverter(rowType, s3Conf);
        } else {
            checkConstant(s3Conf);
            rowConverter = new S3RowConverter(rowType, s3Conf);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createInput(builder.finish());
    }
}
