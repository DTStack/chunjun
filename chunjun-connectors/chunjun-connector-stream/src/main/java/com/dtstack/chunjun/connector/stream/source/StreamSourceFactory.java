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

package com.dtstack.chunjun.connector.stream.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.stream.conf.StreamConf;
import com.dtstack.chunjun.connector.stream.converter.StreamColumnConverter;
import com.dtstack.chunjun.connector.stream.converter.StreamRawTypeConverter;
import com.dtstack.chunjun.connector.stream.converter.StreamRowConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Date: 2021/04/07 Company: www.dtstack.com
 *
 * @author tudou
 */
public class StreamSourceFactory extends SourceFactory {
    private final StreamConf streamConf;

    public StreamSourceFactory(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        streamConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getReader().getParameter()), StreamConf.class);
        streamConf.setColumn(config.getReader().getFieldList());
        super.initCommonConf(streamConf);
    }

    @Override
    public DataStream<RowData> createSource() {
        StreamInputFormatBuilder builder = new StreamInputFormatBuilder();
        builder.setStreamConf(streamConf);
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter = new StreamColumnConverter(streamConf);
        } else {
            checkConstant(streamConf);
            final RowType rowType =
                    TableUtil.createRowType(streamConf.getColumn(), getRawTypeConverter());
            rowConverter = new StreamRowConverter(rowType);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);

        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return StreamRawTypeConverter::apply;
    }
}
