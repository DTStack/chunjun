/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.hbase.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase.converter.HBaseColumnConverter;
import com.dtstack.chunjun.connector.hbase.converter.HBaseFlatRowConverter;
import com.dtstack.chunjun.connector.hbase.converter.HBaseRawTypeConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;
import com.dtstack.chunjun.util.ValueUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class HBaseSinkFactoryBase extends SinkFactory {

    protected final HBaseConf hBaseConf;

    public HBaseSinkFactoryBase(SyncConf config) {
        super(config);
        hBaseConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()), HBaseConf.class);
        super.initCommonConf(hBaseConf);
        hBaseConf.setColumn(syncConf.getWriter().getFieldList());
        hBaseConf.setColumnMetaInfos(syncConf.getReader().getFieldList());

        if (config.getWriter().getParameter().get("rowkeyColumn") != null) {
            String rowkeyColumn =
                    buildRowKeyExpress(config.getWriter().getParameter().get("rowkeyColumn"));
            hBaseConf.setRowkeyExpress(rowkeyColumn);
        }

        if (config.getWriter().getParameter().get("versionColumn") != null) {
            Map<String, Object> versionColumn =
                    (Map<String, Object>) config.getWriter().getParameter().get("versionColumn");
            if (null != versionColumn.get("index")
                    && StringUtils.isNotBlank(versionColumn.get("index").toString())) {
                hBaseConf.setVersionColumnIndex(
                        Integer.valueOf(versionColumn.get("index").toString()));
            }

            if (null != versionColumn.get("value")
                    && StringUtils.isNotBlank(versionColumn.get("value").toString())) {
                hBaseConf.setVersionColumnValue(versionColumn.get("value").toString());
            }
        }
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        HBaseOutputFormatBuilder builder = new HBaseOutputFormatBuilder();
        builder.setConfig(hBaseConf);

        builder.setHbaseConfig(hBaseConf.getHbaseConfig());
        builder.setTableName(hBaseConf.getTable());
        builder.setWriteBufferSize(hBaseConf.getWriteBufferSize());
        // if use transform, use HBaseFlatRowConverter
        final RowType rowType =
                TableUtil.createRowType(hBaseConf.getColumn(), getRawTypeConverter());
        AbstractRowConverter rowConverter =
                useAbstractBaseColumn
                        ? new HBaseColumnConverter(hBaseConf, rowType)
                        : new HBaseFlatRowConverter(hBaseConf, rowType);
        builder.setRowConverter(rowConverter);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return HBaseRawTypeConverter.INSTANCE;
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
                        String.format("$(%s)", hBaseConf.getColumn().get(index).getName()));
                continue;
            }

            String value = (String) item.get("value");
            if (StringUtils.isNotEmpty(value)) {
                expressBuilder.append(value);
            }
        }

        return expressBuilder.toString();
    }

    HBaseTableSchema buildHBaseTableSchema(String tableName, List<FieldConf> fieldConfList) {
        HBaseTableSchema hbaseSchema = new HBaseTableSchema();
        hbaseSchema.setTableName(tableName);
        RawTypeConverter rawTypeConverter = getRawTypeConverter();
        for (FieldConf fieldConf : fieldConfList) {
            String fieldName = fieldConf.getName();
            DataType dataType = rawTypeConverter.apply(fieldConf.getType());
            if ("rowkey".equalsIgnoreCase(fieldName)) {
                hbaseSchema.setRowKey(fieldName, dataType);
            } else if (fieldName.contains(":")) {
                String[] fields = fieldName.split(":");
                hbaseSchema.addColumn(fields[0], fields[1], dataType);
            }
        }
        return hbaseSchema;
    }
}
