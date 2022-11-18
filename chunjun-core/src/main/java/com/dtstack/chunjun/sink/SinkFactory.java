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

import com.dtstack.chunjun.cdc.CdcConfig;
import com.dtstack.chunjun.cdc.config.DDLConfig;
import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.SpeedConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.converter.RawTypeConvertible;
import com.dtstack.chunjun.util.PropertiesUtil;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

/** Abstract specification of Writer Plugin */
public abstract class SinkFactory implements RawTypeConvertible {

    protected SyncConfig syncConfig;
    protected DDLConfig ddlConfig;
    protected boolean useAbstractBaseColumn = true;

    public SinkFactory(SyncConfig syncConfig) {
        this.syncConfig = syncConfig;

        if (syncConfig.getTransformer() != null
                && StringUtils.isNotBlank(syncConfig.getTransformer().getTransformSql())) {
            useAbstractBaseColumn = false;
        }
        CdcConfig cdcConfig = syncConfig.getCdcConf();
        if (cdcConfig != null) {
            ddlConfig = cdcConfig.getDdl();
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

    /** 初始化CommonConfig */
    public void initCommonConf(CommonConfig commonConfig) {
        PropertiesUtil.initCommonConf(commonConfig, this.syncConfig);
        commonConfig.setCheckFormat(this.syncConfig.getWriter().getBooleanVal("check", true));
        SpeedConfig speed = this.syncConfig.getSpeed();
        commonConfig.setParallelism(
                speed.getWriterChannel() == -1 ? speed.getChannel() : speed.getWriterChannel());
    }
}
