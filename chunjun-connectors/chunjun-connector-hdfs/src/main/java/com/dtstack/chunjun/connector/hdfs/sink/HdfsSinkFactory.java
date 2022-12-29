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
package com.dtstack.chunjun.connector.hdfs.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.hdfs.config.HdfsConfig;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsRawTypeConverter;
import com.dtstack.chunjun.connector.hdfs.util.HdfsUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

public class HdfsSinkFactory extends SinkFactory {

    private final HdfsConfig hdfsConfig;

    public HdfsSinkFactory(SyncConfig config) {
        super(config);
        hdfsConfig =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()), HdfsConfig.class);
        hdfsConfig.setColumn(config.getWriter().getFieldList());
        super.initCommonConf(hdfsConfig);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        HdfsOutputFormatBuilder builder =
                HdfsOutputFormatBuilder.newBuild(hdfsConfig.getFileType());
        builder.setHdfsConf(hdfsConfig);
        AbstractRowConverter rowConverter =
                HdfsUtil.createRowConverter(
                        useAbstractBaseColumn,
                        hdfsConfig.getFileType(),
                        hdfsConfig.getColumn(),
                        getRawTypeConverter(),
                        hdfsConfig);

        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return HdfsRawTypeConverter::apply;
    }
}
