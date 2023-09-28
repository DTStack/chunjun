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

package com.dtstack.chunjun.connector.oceanbasecdc.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.oceanbasecdc.config.OceanBaseCdcConfig;
import com.dtstack.chunjun.connector.oceanbasecdc.converter.OceanBaseCdcRawTypeMapper;
import com.dtstack.chunjun.connector.oceanbasecdc.converter.OceanBaseCdcSqlConverter;
import com.dtstack.chunjun.connector.oceanbasecdc.converter.OceanBaseCdcSyncConverter;
import com.dtstack.chunjun.connector.oceanbasecdc.format.TimestampFormat;
import com.dtstack.chunjun.connector.oceanbasecdc.inputformat.OceanBaseCdcInputFormatBuilder;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

@SuppressWarnings("rawtypes")
public class OceanbasecdcSourceFactory extends SourceFactory {

    private final OceanBaseCdcConfig cdcConf;

    public OceanbasecdcSourceFactory(SyncConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        cdcConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(config.getReader().getParameter()),
                        OceanBaseCdcConfig.class);
        cdcConf.setColumn(config.getReader().getFieldList());
        super.initCommonConf(cdcConf);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return OceanBaseCdcRawTypeMapper::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        OceanBaseCdcInputFormatBuilder builder = new OceanBaseCdcInputFormatBuilder();
        builder.setOceanBaseCdcConf(cdcConf);
        AbstractCDCRawTypeMapper rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter =
                    new OceanBaseCdcSyncConverter(cdcConf.isPavingData(), cdcConf.isSplitUpdate());
        } else {
            final RowType rowType =
                    TableUtil.createRowType(cdcConf.getColumn(), getRawTypeMapper());
            TimestampFormat format =
                    "sql".equalsIgnoreCase(cdcConf.getTimestampFormat())
                            ? TimestampFormat.SQL
                            : TimestampFormat.ISO_8601;
            rowConverter = new OceanBaseCdcSqlConverter(rowType, format);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createInput(builder.finish());
    }
}
