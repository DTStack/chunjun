/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.chunjun.connector.hbase14.sink;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.hbase.HBaseColumnConverter;
import com.dtstack.chunjun.connector.hbase14.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase14.converter.HBaseRawTypeConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

public class HBase14SinkFactory extends SinkFactory {

    private final HBaseConf hbaseConf;

    public HBase14SinkFactory(SyncConf config) {
        super(config);
        hbaseConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()), HBaseConf.class);
        super.initCommonConf(hbaseConf);
        hbaseConf.setColumnMetaInfos(syncConf.getWriter().getFieldList());
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        HBaseOutputFormatBuilder builder = new HBaseOutputFormatBuilder();
        builder.setConfig(hbaseConf);
        builder.setColumnMetaInfos(hbaseConf.getColumnMetaInfos());
        builder.setEncoding(hbaseConf.getEncoding());
        builder.setHbaseConfig(hbaseConf.getHbaseConfig());
        builder.setNullMode(hbaseConf.getNullMode());
        builder.setRowkeyExpress(hbaseConf.getRowkeyExpress());
        builder.setTableName(hbaseConf.getTable());
        builder.setVersionColumnIndex(hbaseConf.getVersionColumnIndex());
        builder.setVersionColumnValues(hbaseConf.getVersionColumnValue());
        builder.setWalFlag(hbaseConf.getWalFlag());
        builder.setRowkeyExpress(hbaseConf.getRowkeyExpress());
        builder.setWriteBufferSize(hbaseConf.getWriteBufferSize());
        AbstractRowConverter rowConverter =
                new HBaseColumnConverter(hbaseConf.getColumnMetaInfos());
        builder.setRowConverter(rowConverter);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return new HBaseRawTypeConverter();
    }
}
