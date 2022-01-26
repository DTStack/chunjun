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

package com.dtstack.flinkx.connector.ftp.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.ftp.conf.ConfigConstants;
import com.dtstack.flinkx.connector.ftp.conf.FtpConfig;
import com.dtstack.flinkx.connector.ftp.converter.FtpColumnConverter;
import com.dtstack.flinkx.connector.ftp.converter.FtpRawTypeConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.util.JsonUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/06/19
 */
public class FtpSourceFactory extends SourceFactory {

    private FtpConfig ftpConfig;

    public FtpSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        ftpConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getReader().getParameter()), FtpConfig.class);

        if (ftpConfig.getPort() == null) {
            ftpConfig.setDefaultPort();
        }

        if (!ConfigConstants.DEFAULT_FIELD_DELIMITER.equals(ftpConfig.getFieldDelimiter())) {
            String fieldDelimiter = StringUtil.convertRegularExpr(ftpConfig.getFieldDelimiter());
            ftpConfig.setFieldDelimiter(fieldDelimiter);
        }
        super.initFlinkxCommonConf(ftpConfig);
    }

    @Override
    public DataStream<RowData> createSource() {
        FtpInputFormatBuilder builder = new FtpInputFormatBuilder();
        builder.setFtpConfig(ftpConfig);
        List<FieldConf> fieldConfList =
                ftpConfig.getColumn().stream()
                        .peek(
                                fieldConf -> {
                                    if (fieldConf.getName() == null) {
                                        fieldConf.setName(String.valueOf(fieldConf.getIndex()));
                                    }
                                })
                        .collect(Collectors.toList());
        ftpConfig.setColumn(fieldConfList);
        final RowType rowType =
                TableUtil.createRowType(ftpConfig.getColumn(), getRawTypeConverter());
        builder.setRowConverter(new FtpColumnConverter(rowType, ftpConfig));

        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return FtpRawTypeConverter::apply;
    }
}
