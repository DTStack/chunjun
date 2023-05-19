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

package com.dtstack.chunjun.connector.ftp.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.ftp.config.ConfigConstants;
import com.dtstack.chunjun.connector.ftp.config.FtpConfig;
import com.dtstack.chunjun.connector.ftp.converter.FtpRawTypeMapper;
import com.dtstack.chunjun.connector.ftp.converter.FtpSyncConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.StringUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;

public class FtpSinkFactory extends SinkFactory {

    private final FtpConfig ftpConfig;

    public FtpSinkFactory(SyncConfig syncConfig) {
        super(syncConfig);
        ftpConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConfig.getWriter().getParameter()), FtpConfig.class);

        if (ftpConfig.getPort() == null) {
            ftpConfig.setDefaultPort();
        }

        if (!ConfigConstants.DEFAULT_FIELD_DELIMITER.equals(ftpConfig.getFieldDelimiter())) {
            String fieldDelimiter = StringUtil.convertRegularExpr(ftpConfig.getFieldDelimiter());
            ftpConfig.setFieldDelimiter(fieldDelimiter);
        }
        super.initCommonConf(ftpConfig);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        FtpOutputFormatBuilder builder = new FtpOutputFormatBuilder();
        builder.setConfig(ftpConfig);
        builder.setFtpConfig(ftpConfig);
        List<FieldConfig> fieldConfList =
                ftpConfig.getColumn().stream()
                        .peek(
                                fieldConfig -> {
                                    if (fieldConfig.getName() == null) {
                                        fieldConfig.setName(String.valueOf(fieldConfig.getIndex()));
                                    }
                                })
                        .collect(Collectors.toList());
        ftpConfig.setColumn(fieldConfList);
        final RowType rowType = TableUtil.createRowType(ftpConfig.getColumn(), getRawTypeMapper());
        builder.setRowConverter(new FtpSyncConverter(rowType, ftpConfig), useAbstractBaseColumn);

        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return FtpRawTypeMapper::apply;
    }
}
