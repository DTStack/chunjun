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
package com.dtstack.flinkx.connector.sqlservercdc.source;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.sqlservercdc.conf.SqlServerCdcConf;
import com.dtstack.flinkx.connector.sqlservercdc.convert.SqlServerCdcColumnConverter;
import com.dtstack.flinkx.connector.sqlservercdc.convert.SqlServerCdcRawTypeConverter;
import com.dtstack.flinkx.connector.sqlservercdc.convert.SqlServerCdcRowConverter;
import com.dtstack.flinkx.connector.sqlservercdc.inputFormat.SqlServerCdcInputFormatBuilder;
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
@SuppressWarnings("all")
public class SqlservercdcSourceFactory extends SourceFactory {

    private final SqlServerCdcConf sqlServerCdcConf;

    public SqlservercdcSourceFactory(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        sqlServerCdcConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(config.getReader().getParameter()), SqlServerCdcConf.class);
        sqlServerCdcConf.setColumn(config.getReader().getFieldList());
        super.initFlinkxCommonConf(sqlServerCdcConf);
    }

    @Override
    public DataStream<RowData> createSource() {
        SqlServerCdcInputFormatBuilder builder = new SqlServerCdcInputFormatBuilder();
        builder.setSqlServerCdcConf(sqlServerCdcConf);
        AbstractCDCRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter =
                    new SqlServerCdcColumnConverter(
                            sqlServerCdcConf.isPavingData(), sqlServerCdcConf.isSplitUpdate());
        } else {
            final RowType rowType =
                    (RowType)
                            TableUtil.createRowType(
                                    sqlServerCdcConf.getColumn(), getRawTypeConverter());
            TimestampFormat format =
                    "sql".equalsIgnoreCase(sqlServerCdcConf.getTimestampFormat())
                            ? TimestampFormat.SQL
                            : TimestampFormat.ISO_8601;
            rowConverter = new SqlServerCdcRowConverter(rowType, format);
        }
        builder.setRowConverter(rowConverter);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return SqlServerCdcRawTypeConverter::apply;
    }
}
