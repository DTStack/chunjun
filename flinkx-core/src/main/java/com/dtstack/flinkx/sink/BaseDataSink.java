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

import com.dtstack.flinkx.conf.FlinkxConf;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.source.MetaColumn;
import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;
import com.dtstack.flinkx.util.DataTypeUtil;
import com.dtstack.flinkx.util.TableUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;

/**
 * Abstract specification of Writer Plugin
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class BaseDataSink implements RetractStreamTableSink<Row> {

    protected TableSchema tableSchema;
    protected FlinkxConf config;
    protected TypeInformation<Row> typeInformation;

    @SuppressWarnings("unchecked")
    public BaseDataSink(FlinkxConf config) {
        //脏数据记录reader中的字段信息
        List metaColumn = config.getReader().getMetaColumn();
        if(CollectionUtils.isNotEmpty(metaColumn)){
            config.getDirty().setReaderColumnNameList(MetaColumn.getColumnNameList(metaColumn));
        }
        initColumn(config);
        this.config = config;

        if(config.getTransformer() == null || StringUtils.isBlank(config.getTransformer().getTransformSql())){
            typeInformation = TableUtil.getRowTypeInformation(Collections.emptyList());
        }else{
            typeInformation = TableUtil.getRowTypeInformation(config.getReader().getFieldList());
            tableSchema = TableSchema.builder()
                    .fields(config.getReader().getFieldNameList().toArray(new String[0]), DataTypeUtil.getFieldTypes(config.getReader().getFieldClassList()))
                    .build();
        }
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return writeData(dataStream);
    }

    /**
     * Build the write data flow with read data flow
     *
     * @param dataSet read data flow
     * @return write data flow
     */
    public abstract DataStreamSink<Tuple2<Boolean, Row>> writeData(DataStream<Tuple2<Boolean, Row>> dataSet);

    @SuppressWarnings("unchecked")
    protected DataStreamSink<Tuple2<Boolean, Row>> createOutput(DataStream<Tuple2<Boolean, Row>> dataSet, OutputFormat outputFormat, String sinkName) {
        Preconditions.checkNotNull(dataSet);
        Preconditions.checkNotNull(sinkName);
        Preconditions.checkNotNull(outputFormat);

        DtOutputFormatSinkFunction sinkFunction = new DtOutputFormatSinkFunction(outputFormat);
        DataStreamSink<Tuple2<Boolean, Row>> dataStreamSink = dataSet.addSink(sinkFunction);
        dataStreamSink.name(sinkName);

        return dataStreamSink;
    }

    protected DataStreamSink<Tuple2<Boolean, Row>> createOutput(DataStream<Tuple2<Boolean, Row>> dataSet, OutputFormat outputFormat) {
        return createOutput(dataSet, outputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    /**
     *
     * getMetaColumns(columns, true); 默认对column里index为空时处理为对应数据在数组里的下标而不是-1
     * 如果index为-1是有特殊逻辑 需要覆盖此方法使用 getMetaColumns(List columns, false) 代替
     * @param config 配置信息
     */
    protected void initColumn(FlinkxConf config){
        List<MetaColumn> writerColumnList = MetaColumn.getMetaColumns(config.getWriter().getMetaColumn());
        if(CollectionUtils.isNotEmpty(writerColumnList)){
            config.getWriter().getParameter().put(ConfigConstant.KEY_COLUMN, writerColumnList);
        }
    }

    @Override
    public TableSchema getTableSchema() {
        return this.tableSchema;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return this;
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return getTableSchema().toRowType();
    }
}
