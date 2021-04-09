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
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.source.MetaColumn;
import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;
import com.dtstack.flinkx.util.PropertiesUtil;
import com.dtstack.flinkx.util.TableUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * Abstract specification of Writer Plugin
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class BaseDataSink {

    protected SyncConf syncConf;
    protected TypeInformation<RowData> typeInformation;

    @SuppressWarnings("unchecked")
    public BaseDataSink(SyncConf syncConf) {
        //脏数据记录reader中的字段信息
        List metaColumn = syncConf.getReader().getMetaColumn();
        if(CollectionUtils.isNotEmpty(metaColumn)){
            syncConf.getDirty().setReaderColumnNameList(MetaColumn.getColumnNameList(metaColumn));
        }
        initColumn(syncConf);
        this.syncConf = syncConf;

        if(syncConf.getTransformer() == null || StringUtils.isBlank(syncConf.getTransformer().getTransformSql())){
            typeInformation = TableUtil.getTypeInformation(Collections.emptyList());
        }else{
            typeInformation = TableUtil.getTypeInformation(syncConf.getReader().getFieldList());
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

        ((BaseRichOutputFormat)outputFormat).setBatchSize((int) syncConf
                .getWriter().getParameter().getOrDefault(ConfigConstant.KEY_BATCH_SIZE, 1));

        DtOutputFormatSinkFunction sinkFunction = new DtOutputFormatSinkFunction(outputFormat);
        DataStreamSink<RowData> dataStreamSink = dataSet.addSink(sinkFunction);
        dataStreamSink.name(sinkName);

        return dataStreamSink;
    }

    protected DataStreamSink<RowData> createOutput(DataStream<RowData> dataSet, OutputFormat outputFormat) {
        return createOutput(dataSet, outputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    /**
     *
     * getMetaColumns(columns, true); 默认对column里index为空时处理为对应数据在数组里的下标而不是-1
     * 如果index为-1是有特殊逻辑 需要覆盖此方法使用 getMetaColumns(List columns, false) 代替
     * @param config 配置信息
     */
    protected void initColumn(SyncConf config){
        List<MetaColumn> writerColumnList = MetaColumn.getMetaColumns(config.getWriter().getMetaColumn());
        if(CollectionUtils.isNotEmpty(writerColumnList)){
            config.getWriter().getParameter().put(ConfigConstant.KEY_COLUMN, writerColumnList);
        }
    }

    /**
     * 初始化FlinkxCommonConf
     * @param flinkxCommonConf
     */
    public void initFlinkxCommonConf(FlinkxCommonConf flinkxCommonConf){
        PropertiesUtil.initFlinkxCommonConf(flinkxCommonConf, this.syncConf);
        flinkxCommonConf.setCheckFormat(this.syncConf.getWriter().getBooleanVal("check", true));
    }
}
