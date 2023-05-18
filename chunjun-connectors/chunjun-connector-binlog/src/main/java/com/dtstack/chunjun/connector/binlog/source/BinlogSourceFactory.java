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

package com.dtstack.chunjun.connector.binlog.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.binlog.config.BinlogConfig;
import com.dtstack.chunjun.connector.binlog.converter.BinlogSqlConverter;
import com.dtstack.chunjun.connector.binlog.converter.BinlogSyncConverter;
import com.dtstack.chunjun.connector.binlog.converter.MysqlBinlogRawTypeMapper;
import com.dtstack.chunjun.connector.binlog.format.TimestampFormat;
import com.dtstack.chunjun.connector.binlog.inputformat.BinlogInputFormatBuilder;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class BinlogSourceFactory extends SourceFactory {

    private final BinlogConfig binlogConfig;

    public BinlogSourceFactory(SyncConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        binlogConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(config.getReader().getParameter()), BinlogConfig.class);
        binlogConfig.setColumn(config.getReader().getFieldList());
        super.initCommonConf(binlogConfig);
    }

    @Override
    public DataStream<RowData> createSource() {
        BinlogInputFormatBuilder builder = new BinlogInputFormatBuilder();
        builder.setBinlogConf(binlogConfig);
        AbstractCDCRawTypeMapper rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter =
                    new BinlogSyncConverter(binlogConfig.isPavingData(), binlogConfig.isSplit());
        } else {
            final RowType rowType =
                    TableUtil.createRowType(binlogConfig.getColumn(), getRawTypeMapper());
            TimestampFormat format =
                    "sql".equalsIgnoreCase(binlogConfig.getTimestampFormat())
                            ? TimestampFormat.SQL
                            : TimestampFormat.ISO_8601;
            rowConverter = new BinlogSqlConverter(rowType, format);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return MysqlBinlogRawTypeMapper::apply;
    }
}
