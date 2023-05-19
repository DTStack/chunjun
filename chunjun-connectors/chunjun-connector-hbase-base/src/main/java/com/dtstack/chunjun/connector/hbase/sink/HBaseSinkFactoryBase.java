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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.connector.hbase.converter.HBaseFlatRowConverter;
import com.dtstack.chunjun.connector.hbase.converter.HBaseRawTypeMapper;
import com.dtstack.chunjun.connector.hbase.converter.HBaseSyncConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
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

    protected final HBaseConfig hBaseConfig;

    public HBaseSinkFactoryBase(SyncConfig config) {
        super(config);
        hBaseConfig =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()), HBaseConfig.class);
        super.initCommonConf(hBaseConfig);
        hBaseConfig.setColumn(syncConfig.getWriter().getFieldList());
        hBaseConfig.setColumnMetaInfos(syncConfig.getReader().getFieldList());

        if (config.getWriter().getParameter().get("rowkeyColumn") != null) {
            String rowkeyColumn =
                    buildRowKeyExpress(config.getWriter().getParameter().get("rowkeyColumn"));
            hBaseConfig.setRowkeyExpress(rowkeyColumn);
        }

        if (config.getWriter().getParameter().get("versionColumn") != null) {
            Map<String, Object> versionColumn =
                    (Map<String, Object>) config.getWriter().getParameter().get("versionColumn");
            if (null != versionColumn.get("index")
                    && StringUtils.isNotBlank(versionColumn.get("index").toString())) {
                hBaseConfig.setVersionColumnIndex(
                        Integer.valueOf(versionColumn.get("index").toString()));
            }

            if (null != versionColumn.get("value")
                    && StringUtils.isNotBlank(versionColumn.get("value").toString())) {
                hBaseConfig.setVersionColumnValue(versionColumn.get("value").toString());
            }
        }
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        HBaseOutputFormatBuilder builder = new HBaseOutputFormatBuilder();
        builder.setConfig(hBaseConfig);

        builder.setHbaseConfig(hBaseConfig.getHbaseConfig());
        builder.setTableName(hBaseConfig.getTable());
        builder.setWriteBufferSize(hBaseConfig.getWriteBufferSize());
        // if you use transform, use HBaseFlatRowConverter
        final RowType rowType =
                TableUtil.createRowType(hBaseConfig.getColumn(), getRawTypeMapper());
        AbstractRowConverter<?, ?, ?, ?> rowConverter =
                useAbstractBaseColumn
                        ? new HBaseSyncConverter(hBaseConfig, rowType)
                        : new HBaseFlatRowConverter(hBaseConfig, rowType);
        builder.setRowConverter(rowConverter);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return HBaseRawTypeMapper.INSTANCE;
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
                        String.format("$(%s)", hBaseConfig.getColumn().get(index).getName()));
                continue;
            }

            String value = (String) item.get("value");
            if (StringUtils.isNotEmpty(value)) {
                expressBuilder.append(value);
            }
        }

        return expressBuilder.toString();
    }

    HBaseTableSchema buildHBaseTableSchema(String tableName, List<FieldConfig> FieldConfigList) {
        HBaseTableSchema hbaseSchema = new HBaseTableSchema();
        hbaseSchema.setTableName(tableName);
        RawTypeMapper rawTypeMapper = getRawTypeMapper();
        for (FieldConfig config : FieldConfigList) {
            String fieldName = config.getName();
            DataType dataType = rawTypeMapper.apply(config.getType());
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
