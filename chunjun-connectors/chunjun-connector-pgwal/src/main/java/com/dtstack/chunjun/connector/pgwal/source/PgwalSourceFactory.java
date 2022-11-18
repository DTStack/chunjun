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

package com.dtstack.chunjun.connector.pgwal.source;

import com.dtstack.chunjun.config.SyncConf;
import com.dtstack.chunjun.connector.pgwal.conf.PGWalConf;
import com.dtstack.chunjun.connector.pgwal.converter.PGWalColumnConverter;
import com.dtstack.chunjun.connector.pgwal.converter.PGWalRowConverter;
import com.dtstack.chunjun.connector.pgwal.inputformat.PGWalInputFormatBuilder;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class PgwalSourceFactory extends SourceFactory {

    private final PGWalConf conf;

    public PgwalSourceFactory(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        conf =
                JsonUtil.toObject(
                        JsonUtil.toJson(config.getReader().getParameter()), PGWalConf.class);
        conf.setColumn(config.getReader().getFieldList());
        super.initCommonConf(conf);
    }

    @Override
    public DataStream<RowData> createSource() {
        PGWalInputFormatBuilder builder = new PGWalInputFormatBuilder();
        builder.setConf(conf);
        AbstractCDCRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter = new PGWalColumnConverter(conf.isPavingData(), false);
        } else {
            final RowType rowType =
                    TableUtil.createRowType(conf.getColumn(), getRawTypeConverter());
            rowConverter = new PGWalRowConverter(rowType, TimestampFormat.SQL);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }
}
