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
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase.converter.HBaseRawTypeConverter;
import com.dtstack.chunjun.connector.hbase14.converter.HBaseColumnConverter;
import com.dtstack.chunjun.connector.hbase14.converter.HbaseRowConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;
import com.dtstack.chunjun.util.ValueUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

public class HBase14SinkFactory extends SinkFactory {

    private final HBaseConf hbaseConf;

    public HBase14SinkFactory(SyncConf config) {
        super(config);
        hbaseConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()), HBaseConf.class);
        super.initCommonConf(hbaseConf);
        hbaseConf.setColumn(syncConf.getWriter().getFieldList());

        if (config.getWriter().getParameter().get("rowkeyColumn") != null) {
            String rowkeyColumn =
                    buildRowKeyExpress(config.getWriter().getParameter().get("rowkeyColumn"));
            hbaseConf.setRowkeyExpress(rowkeyColumn);
        }

        if (config.getWriter().getParameter().get("versionColumn") != null) {
            Map<String, Object> versionColumn =
                    (Map<String, Object>) config.getWriter().getParameter().get("versionColumn");
            if (null != versionColumn.get("index")
                    && StringUtils.isNotBlank(versionColumn.get("index").toString())) {
                hbaseConf.setVersionColumnIndex(
                        Integer.valueOf(versionColumn.get("index").toString()));
            }

            if (null != versionColumn.get("value")
                    && StringUtils.isNotBlank(versionColumn.get("value").toString())) {
                hbaseConf.setVersionColumnValue(versionColumn.get("value").toString());
            }
        }
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        HBaseOutputFormatBuilder builder = new HBaseOutputFormatBuilder();
        builder.setConfig(hbaseConf);
        builder.setHbaseConf(hbaseConf);

        builder.setHbaseConfig(hbaseConf.getHbaseConfig());
        builder.setTableName(hbaseConf.getTable());
        builder.setWriteBufferSize(hbaseConf.getWriteBufferSize());
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            final RowType rowType =
                    TableUtil.createRowType(hbaseConf.getColumn(), getRawTypeConverter());
            rowConverter = new HBaseColumnConverter(hbaseConf, rowType);
        } else {
            TableSchema tableSchema =
                    TableUtil.createTableSchema(hbaseConf.getColumn(), getRawTypeConverter());
            HBaseTableSchema hbaseSchema = HBaseTableSchema.fromTableSchema(tableSchema);
            String nullStringLiteral = hbaseConf.getNullStringLiteral();
            rowConverter = new HbaseRowConverter(hbaseSchema, nullStringLiteral);
        }

        builder.setRowConverter(rowConverter);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return HBaseRawTypeConverter::apply;
    }

    /** Compatible with old formats */
    private String buildRowKeyExpress(Object rowKeyInfo) {
        if (rowKeyInfo == null) {
            return null;
        }

        if (rowKeyInfo instanceof String) {
            return rowKeyInfo.toString();
        }

        if (!(rowKeyInfo instanceof List)) {
            return null;
        }

        StringBuilder expressBuilder = new StringBuilder();

        for (Map item : ((List<Map>) rowKeyInfo)) {
            Integer index = ValueUtil.getInt(item.get("index"));
            if (index != null && index != -1) {
                expressBuilder.append(
                        String.format("$(%s)", hbaseConf.getColumn().get(index).getName()));
                continue;
            }

            String value = (String) item.get("value");
            if (StringUtils.isNotEmpty(value)) {
                expressBuilder.append(value);
            }
        }

        return expressBuilder.toString();
    }
}
