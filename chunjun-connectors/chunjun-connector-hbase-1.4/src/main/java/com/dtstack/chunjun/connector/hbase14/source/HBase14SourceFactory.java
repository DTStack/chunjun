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
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase.converter.HBaseRawTypeConverter;
import com.dtstack.chunjun.connector.hbase14.converter.HBaseColumnConverter;
import com.dtstack.chunjun.connector.hbase14.converter.HbaseRowConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HBase14SourceFactory extends SourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HBase14SourceFactory.class);

    private final HBaseConf config;

    public HBase14SourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        config =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(syncConf.getReader().getParameter()), HBaseConf.class);
        Map<String, Object> range =
                (Map<String, Object>) syncConf.getReader().getParameter().get("range");
        if (range != null) {
            if (range.get("startRowkey") != null
                    && StringUtils.isNotBlank(range.get("startRowkey").toString())) {
                config.setStartRowkey(range.get("startRowkey").toString());
            }
            if (range.get("endRowkey") != null
                    && StringUtils.isNotBlank(range.get("endRowkey").toString())) {
                config.setEndRowkey(range.get("endRowkey").toString());
            }
            if (range.get("isBinaryRowkey") != null) {
                config.setBinaryRowkey((Boolean) range.get("isBinaryRowkey"));
            }
        }

        super.initCommonConf(config);
        config.setColumn(syncConf.getReader().getFieldList());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return HBaseRawTypeConverter::apply;
    }

    @Override
    @SuppressWarnings("all")
    public DataStream<RowData> createSource() {
        HBaseInputFormatBuilder builder = new HBaseInputFormatBuilder();
        builder.setConfig(config);
        builder.sethHBaseConf(config);

        builder.setHbaseConfig(config.getHbaseConfig());

        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            final RowType rowType =
                    TableUtil.createRowType(config.getColumn(), getRawTypeConverter());
            rowConverter = new HBaseColumnConverter(config, rowType);
        } else {
            TableSchema tableSchema =
                    TableUtil.createTableSchema(config.getColumn(), getRawTypeConverter());
            HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(tableSchema);
            String nullStringLiteral = config.getNullStringLiteral();
            rowConverter = new HbaseRowConverter(hbaseSchema, nullStringLiteral);
        }

        builder.setRowConverter(rowConverter);
        return createInput(builder.finish());
    }
}
