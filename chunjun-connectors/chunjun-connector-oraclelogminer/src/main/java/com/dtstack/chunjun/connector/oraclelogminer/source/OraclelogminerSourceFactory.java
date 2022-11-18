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

import com.dtstack.chunjun.config.SyncConf;
import com.dtstack.chunjun.connector.oraclelogminer.conf.LogMinerConf;
import com.dtstack.chunjun.connector.oraclelogminer.converter.LogMinerColumnConverter;
import com.dtstack.chunjun.connector.oraclelogminer.converter.LogMinerRowConverter;
import com.dtstack.chunjun.connector.oraclelogminer.converter.OracleRawTypeConverter;
import com.dtstack.chunjun.connector.oraclelogminer.inputformat.OracleLogMinerInputFormatBuilder;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
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

    private final LogMinerConf logMinerConf;

    public OraclelogminerSourceFactory(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        logMinerConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(config.getReader().getParameter()), LogMinerConf.class);
        logMinerConf.setColumn(config.getReader().getFieldList());
        buildTableListenerRegex();
        super.initCommonConf(logMinerConf);
    }

    private void buildTableListenerRegex() {
        if (CollectionUtils.isEmpty(logMinerConf.getTable())) {
            return;
        }

        String tableListener = StringUtils.join(logMinerConf.getTable(), ",");
        logMinerConf.setListenerTables(tableListener);
    }

    @Override
    public DataStream<RowData> createSource() {
        OracleLogMinerInputFormatBuilder builder = new OracleLogMinerInputFormatBuilder();
        builder.setLogMinerConfig(logMinerConf);
        AbstractCDCRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter =
                    new LogMinerColumnConverter(
                            logMinerConf.isPavingData(), logMinerConf.isSplit());
        } else {
            final RowType rowType =
                    TableUtil.createRowType(logMinerConf.getColumn(), getRawTypeConverter());
            rowConverter = new LogMinerRowConverter(rowType);
        }
        builder.setRowConverter(rowConverter);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return OracleRawTypeConverter::apply;
    }
}
