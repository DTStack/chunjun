/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.connector.binlog.source;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.binlog.conf.BinlogConf;
import com.dtstack.flinkx.connector.binlog.converter.BinlogColumnConverter;
import com.dtstack.flinkx.connector.binlog.converter.BinlogRowConverter;
import com.dtstack.flinkx.connector.binlog.converter.MysqlBinlogRawTypeConverter;
import com.dtstack.flinkx.connector.binlog.inputformat.BinlogInputFormatBuilder;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.util.JsonUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/4
 */
public class BinlogSourceFactory extends SourceFactory {

    private final BinlogConf binlogConf;

    public BinlogSourceFactory(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        binlogConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(config.getReader().getParameter()), BinlogConf.class);
        binlogConf.setColumn(config.getReader().getFieldList());
        super.initFlinkxCommonConf(binlogConf);
    }

    @Override
    public DataStream<RowData> createSource() {
        BinlogInputFormatBuilder builder = new BinlogInputFormatBuilder();
        builder.setBinlogConf(binlogConf);
        AbstractCDCRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter =
                    new BinlogColumnConverter(
                            binlogConf.isPavingData(), binlogConf.isSplit());
        } else {
            final RowType rowType =
                    TableUtil.createRowType(binlogConf.getColumn(), getRawTypeConverter());
            TimestampFormat format =
                    "sql".equalsIgnoreCase(binlogConf.getTimestampFormat())
                            ? TimestampFormat.SQL
                            : TimestampFormat.ISO_8601;
            rowConverter = new BinlogRowConverter(rowType, format);
        }
        builder.setRowConverter(rowConverter);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return MysqlBinlogRawTypeConverter::apply;
    }
}
