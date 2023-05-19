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

package com.dtstack.chunjun.connector.sqlservercdc.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.sqlservercdc.config.SqlServerCdcConfig;
import com.dtstack.chunjun.connector.sqlservercdc.convert.SqlServerCdcRawTypeMapper;
import com.dtstack.chunjun.connector.sqlservercdc.convert.SqlServerCdcSqlConverter;
import com.dtstack.chunjun.connector.sqlservercdc.convert.SqlServerCdcSyncConverter;
import com.dtstack.chunjun.connector.sqlservercdc.format.TimestampFormat;
import com.dtstack.chunjun.connector.sqlservercdc.inputFormat.SqlServerCdcInputFormatBuilder;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class SqlservercdcSourceFactory extends SourceFactory {

    private final SqlServerCdcConfig sqlServerCdcConfig;

    public SqlservercdcSourceFactory(SyncConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        sqlServerCdcConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(config.getReader().getParameter()),
                        SqlServerCdcConfig.class);
        sqlServerCdcConfig.setColumn(config.getReader().getFieldList());
        super.initCommonConf(sqlServerCdcConfig);
    }

    @Override
    public DataStream<RowData> createSource() {
        SqlServerCdcInputFormatBuilder builder = new SqlServerCdcInputFormatBuilder();
        builder.setSqlServerCdcConf(sqlServerCdcConfig);
        AbstractCDCRawTypeMapper rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter =
                    new SqlServerCdcSyncConverter(
                            sqlServerCdcConfig.isPavingData(), sqlServerCdcConfig.isSplitUpdate());
        } else {
            final RowType rowType =
                    TableUtil.createRowType(sqlServerCdcConfig.getColumn(), getRawTypeMapper());
            TimestampFormat format =
                    "sql".equalsIgnoreCase(sqlServerCdcConfig.getTimestampFormat())
                            ? TimestampFormat.SQL
                            : TimestampFormat.ISO_8601;
            rowConverter = new SqlServerCdcSqlConverter(rowType, format);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return SqlServerCdcRawTypeMapper::apply;
    }
}
