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
package com.dtstack.chunjun.connector.hbase14.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.hbase.HBaseColumnConverter;
import com.dtstack.chunjun.connector.hbase14.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase14.converter.HBaseRawTypeConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBase14SourceFactory extends SourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HBase14SourceFactory.class);

    private final HBaseConf config;

    public HBase14SourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        config =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(syncConf.getReader().getParameter()), HBaseConf.class);
        super.initCommonConf(config);
        config.setColumnMetaInfos(syncConf.getReader().getFieldList());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return new HBaseRawTypeConverter();
    }

    @Override
    @SuppressWarnings("all")
    public DataStream<RowData> createSource() {
        HBaseInputFormatBuilder builder = new HBaseInputFormatBuilder();
        builder.setColumnMetaInfos(config.getColumnMetaInfos());
        builder.setConfig(config);
        builder.setColumnMetaInfos(config.getColumnMetaInfos());
        builder.setEncoding(config.getEncoding());
        builder.setHbaseConfig(config.getHbaseConfig());
        builder.setTableName(config.getTable());
        builder.setEndRowKey(config.getEndRowkey());
        builder.setIsBinaryRowkey(config.isBinaryRowkey());
        builder.setScanCacheSize(config.getScanCacheSize());
        builder.setStartRowKey(config.getStartRowkey());
        AbstractRowConverter rowConverter = new HBaseColumnConverter(config.getColumnMetaInfos());
        builder.setRowConverter(rowConverter);
        return createInput(builder.finish());
    }
}
