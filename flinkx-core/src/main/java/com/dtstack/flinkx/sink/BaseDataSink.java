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

package com.dtstack.flinkx.sink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.conf.SpeedConf;
import com.dtstack.flinkx.conf.FlinkXConf;
import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;
import com.dtstack.flinkx.util.PropertiesUtil;
import com.dtstack.flinkx.util.TableUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * Abstract specification of Writer Plugin
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class BaseDataSink {

    protected FlinkXConf flinkXConf;
    protected TypeInformation<RowData> typeInformation;

    @SuppressWarnings("unchecked")
    public BaseDataSink(FlinkXConf flinkXConf) {
        //脏数据记录reader中的字段信息
        List<FieldConf> fieldList = flinkXConf.getWriter().getFieldList();
        if(CollectionUtils.isNotEmpty(fieldList)){
            flinkXConf.getDirty().setReaderColumnNameList(flinkXConf.getWriter().getFieldNameList());
        }
        this.flinkXConf = flinkXConf;

        if(flinkXConf.getTransformer() == null || StringUtils.isBlank(flinkXConf.getTransformer().getTransformSql())){
            typeInformation = TableUtil.getTypeInformation(Collections.emptyList());
        }else{
            typeInformation = TableUtil.getTypeInformation(fieldList);
        }
    }

    /**
     * Build the write data flow with read data flow
     *
     * @param dataSet read data flow
     * @return write data flow
     */
    public abstract DataStreamSink<RowData> writeData(DataStream<RowData> dataSet);

    @SuppressWarnings("unchecked")
    protected DataStreamSink<RowData> createOutput(DataStream<RowData> dataSet, OutputFormat outputFormat, String sinkName) {
        Preconditions.checkNotNull(dataSet);
        Preconditions.checkNotNull(sinkName);
        Preconditions.checkNotNull(outputFormat);

        DtOutputFormatSinkFunction sinkFunction = new DtOutputFormatSinkFunction(outputFormat);
        DataStreamSink<RowData> dataStreamSink = dataSet.addSink(sinkFunction);
        dataStreamSink.name(sinkName);

        return dataStreamSink;
    }

    protected DataStreamSink<RowData> createOutput(DataStream<RowData> dataSet, OutputFormat outputFormat) {
        return createOutput(dataSet, outputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    /**
     * 初始化FlinkxCommonConf
     * @param flinkxCommonConf
     */
    public void initFlinkxCommonConf(FlinkxCommonConf flinkxCommonConf){
        PropertiesUtil.initFlinkxCommonConf(flinkxCommonConf, this.flinkXConf);
        flinkxCommonConf.setCheckFormat(this.flinkXConf.getWriter().getBooleanVal("check", true));
        SpeedConf speed = this.flinkXConf.getSpeed();
        flinkxCommonConf.setParallelism(speed.getWriterChannel() == -1 ? speed.getChannel() : speed.getWriterChannel());
    }

    public abstract LogicalType getLogicalType() throws SQLException;
}
