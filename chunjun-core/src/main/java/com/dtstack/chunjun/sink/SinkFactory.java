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

package com.dtstack.chunjun.sink;

import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.conf.SpeedConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.converter.RawTypeConvertible;
import com.dtstack.chunjun.util.PropertiesUtil;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

/**
 * Abstract specification of Writer Plugin
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public abstract class SinkFactory implements RawTypeConvertible {

    protected SyncConf syncConf;
    protected boolean useAbstractBaseColumn = true;

    public SinkFactory(SyncConf syncConf) {
        this.syncConf = syncConf;

        if (syncConf.getTransformer() != null
                && StringUtils.isNotBlank(syncConf.getTransformer().getTransformSql())) {
            useAbstractBaseColumn = false;
        }
    }

    /**
     * Build the write data flow with read data flow
     *
     * @param dataSet read data flow
     * @return write data flow
     */
    public abstract DataStreamSink<RowData> createSink(DataStream<RowData> dataSet);

    protected DataStreamSink<RowData> createOutput(
            DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat, String sinkName) {
        Preconditions.checkNotNull(dataSet);
        Preconditions.checkNotNull(sinkName);
        Preconditions.checkNotNull(outputFormat);

        DtOutputFormatSinkFunction<RowData> sinkFunction =
                new DtOutputFormatSinkFunction<>(outputFormat);
        DataStreamSink<RowData> dataStreamSink = dataSet.addSink(sinkFunction);
        dataStreamSink.name(sinkName);

        return dataStreamSink;
    }

    protected DataStreamSink<RowData> createOutput(
            DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat) {
        return createOutput(dataSet, outputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    /** 初始化ChunJunCommonConf */
    public void initCommonConf(ChunJunCommonConf commonConf) {
        PropertiesUtil.initCommonConf(commonConf, this.syncConf);
        commonConf.setCheckFormat(this.syncConf.getWriter().getBooleanVal("check", true));
        SpeedConf speed = this.syncConf.getSpeed();
        commonConf.setParallelism(
                speed.getWriterChannel() == -1 ? speed.getChannel() : speed.getWriterChannel());
    }
}
