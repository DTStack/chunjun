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

package com.dtstack.chunjun.connector.oraclelogminer.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.oraclelogminer.config.LogMinerConfig;
import com.dtstack.chunjun.connector.oraclelogminer.converter.LogMinerColumnConverter;
import com.dtstack.chunjun.connector.oraclelogminer.converter.LogMinerRawTypeMapper;
import com.dtstack.chunjun.connector.oraclelogminer.converter.OracleRawTypeMapper;
import com.dtstack.chunjun.connector.oraclelogminer.inputformat.OracleLogMinerInputFormatBuilder;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class OraclelogminerSourceFactory extends SourceFactory {

    private final LogMinerConfig logMinerConfig;

    public OraclelogminerSourceFactory(SyncConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        logMinerConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(config.getReader().getParameter()), LogMinerConfig.class);
        logMinerConfig.setColumn(config.getReader().getFieldList());
        buildTableListenerRegex();
        super.initCommonConf(logMinerConfig);
    }

    private void buildTableListenerRegex() {
        if (CollectionUtils.isEmpty(logMinerConfig.getTable())) {
            return;
        }

        String tableListener = StringUtils.join(logMinerConfig.getTable(), ",");
        logMinerConfig.setListenerTables(tableListener);
    }

    @Override
    public DataStream<RowData> createSource() {
        OracleLogMinerInputFormatBuilder builder = new OracleLogMinerInputFormatBuilder();
        builder.setLogMinerConfig(logMinerConfig);
        AbstractCDCRawTypeMapper rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter =
                    new LogMinerColumnConverter(
                            logMinerConfig.isPavingData(), logMinerConfig.isSplit());
        } else {
            final RowType rowType =
                    TableUtil.createRowType(logMinerConfig.getColumn(), getRawTypeMapper());
            rowConverter = new LogMinerRawTypeMapper(rowType);
        }
        builder.setRowConverter(rowConverter);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return OracleRawTypeMapper::apply;
    }
}
