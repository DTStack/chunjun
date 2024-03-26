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

package com.dtstack.chunjun.connector.hbase.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.OperatorConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.connector.hbase.converter.HBaseRawTypeMapper;
import com.dtstack.chunjun.connector.hbase.converter.HBaseSqlConverter;
import com.dtstack.chunjun.connector.hbase.converter.HBaseSyncConverter;
import com.dtstack.chunjun.connector.hbase.util.ScanBuilder;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HBaseSourceFactoryBase extends SourceFactory {

    protected final HBaseConfig hBaseConfig;

    public HBaseSourceFactoryBase(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env);
        OperatorConfig reader = syncConfig.getReader();
        hBaseConfig =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(reader.getParameter()), HBaseConfig.class);
        super.initCommonConf(hBaseConfig);
        hBaseConfig.setColumnMetaInfos(reader.getFieldList());
        Map<String, Object> range =
                (Map<String, Object>) syncConfig.getReader().getParameter().get("range");
        if (range != null) {
            if (range.get("startRowkey") != null
                    && StringUtils.isNotBlank(range.get("startRowkey").toString())) {
                hBaseConfig.setStartRowkey(range.get("startRowkey").toString());
            }
            if (range.get("endRowkey") != null
                    && StringUtils.isNotBlank(range.get("endRowkey").toString())) {
                hBaseConfig.setEndRowkey(range.get("endRowkey").toString());
            }
            if (range.get("isBinaryRowkey") != null) {
                hBaseConfig.setBinaryRowkey((Boolean) range.get("isBinaryRowkey"));
            }
        }
        typeInformation =
                TableUtil.getTypeInformation(
                        fieldList.stream()
                                .peek(
                                        fieldConfig ->
                                                fieldConfig.setName(
                                                        fieldConfig.getName().replace(":", ".")))
                                .collect(Collectors.toList()),
                        getRawTypeMapper(),
                        useAbstractBaseColumn);
        if (hBaseConfig.getNullStringLiteral() == null
                || "".equals(hBaseConfig.getNullStringLiteral().trim())) {
            hBaseConfig.setNullStringLiteral("null");
        }
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return HBaseRawTypeMapper.INSTANCE;
    }

    @Override
    public DataStream<RowData> createSource() {
        List<FieldConfig> fieldConfList = hBaseConfig.getColumnMetaInfos();

        AbstractRowConverter rowConverter;
        ScanBuilder scanBuilder;
        if (useAbstractBaseColumn) {
            final RowType rowType =
                    TableUtil.createRowType(hBaseConfig.getColumn(), getRawTypeMapper());
            rowConverter = new HBaseSyncConverter(hBaseConfig, rowType);
            scanBuilder = ScanBuilder.forSync(fieldConfList);
        } else {
            Set<String> columnSet = new LinkedHashSet<>();
            for (FieldConfig fieldConfig : fieldConfList) {
                String fieldName = fieldConfig.getName();
                if ("rowkey".equalsIgnoreCase(fieldName)) {
                    columnSet.add(fieldName);
                } else if (fieldName.contains(".")) {
                    String[] familyQualifier = fieldName.split("\\.");
                    columnSet.add(familyQualifier[0]);
                }
            }
            this.typeInformation = buildType(columnSet);
            HBaseTableSchema hBaseTableSchema =
                    buildHBaseTableSchema(hBaseConfig.getTable(), fieldConfList);
            syncConfig.getReader().setFieldNameList(new ArrayList<>(columnSet));
            rowConverter = new HBaseSqlConverter(hBaseTableSchema, hBaseConfig);
            scanBuilder = ScanBuilder.forSql(hBaseTableSchema);
        }

        HBaseInputFormatBuilder builder =
                HBaseInputFormatBuilder.newBuild(hBaseConfig.getTable(), scanBuilder);

        builder.setConfig(hBaseConfig);
        builder.setColumnMetaInfos(hBaseConfig.getColumnMetaInfos());
        builder.setHbaseConfig(hBaseConfig.getHbaseConfig());
        builder.setEndRowKey(hBaseConfig.getEndRowkey());
        builder.setIsBinaryRowkey(hBaseConfig.isBinaryRowkey());
        builder.setScanCacheSize(hBaseConfig.getScanCacheSize());
        builder.setStartRowKey(hBaseConfig.getStartRowkey());

        builder.setRowConverter(rowConverter);
        return createInput(builder.finish());
    }

    private TypeInformation<RowData> buildType(Set<String> columnSet) {
        if (columnSet.size() == 0) {
            return new GenericTypeInfo<>(RowData.class);
        }
        Map<String, Map<String, DataType>> family = new LinkedHashMap<>();
        for (String colName : columnSet) {
            if (!"rowkey".equalsIgnoreCase(colName)) {
                family.put(colName, new LinkedHashMap<>());
            }
        }

        TableSchema.Builder builder = TableSchema.builder();
        for (FieldConfig fieldConfig : fieldList) {
            String fieldName = fieldConfig.getName();
            if ("rowkey".equalsIgnoreCase(fieldConfig.getName())) {
                DataType dataType = this.getRawTypeMapper().apply(fieldConfig.getType());
                builder.add(TableColumn.physical(fieldName, dataType));
            } else if (fieldName.contains(".")) {
                String[] familyQualifier = fieldName.split("\\.");
                Map<String, DataType> qualifier = family.get(familyQualifier[0]);
                qualifier.put(
                        familyQualifier[1], this.getRawTypeMapper().apply(fieldConfig.getType()));
            }
        }
        for (Map.Entry<String, Map<String, DataType>> familyQualifierEntry : family.entrySet()) {
            String familyName = familyQualifierEntry.getKey();
            List<DataTypes.Field> rowFieldList = new ArrayList<>();
            for (Map.Entry<String, DataType> qualifierEntry :
                    familyQualifierEntry.getValue().entrySet()) {
                String qualifierName = qualifierEntry.getKey();
                DataTypes.Field rowField =
                        DataTypes.FIELD(qualifierName, qualifierEntry.getValue());
                rowFieldList.add(rowField);
            }
            builder.add(
                    TableColumn.physical(
                            familyName,
                            DataTypes.ROW(rowFieldList.toArray(new DataTypes.Field[] {}))));
        }
        TableSchema tableSchema = builder.build();
        DataType[] dataTypes = tableSchema.toRowDataType().getChildren().toArray(new DataType[] {});
        return TableUtil.getTypeInformation(dataTypes, tableSchema.getFieldNames());
    }

    HBaseTableSchema buildHBaseTableSchema(String tableName, List<FieldConfig> fieldConfList) {
        HBaseTableSchema hbaseSchema = new HBaseTableSchema();
        hbaseSchema.setTableName(tableName);
        RawTypeMapper rawTypeMapper = getRawTypeMapper();
        for (FieldConfig fieldConfig : fieldConfList) {
            String fieldName = fieldConfig.getName();
            DataType dataType = rawTypeMapper.apply(fieldConfig.getType());
            if ("rowkey".equalsIgnoreCase(fieldName)) {
                hbaseSchema.setRowKey(fieldName, dataType);
            } else if (fieldName.contains(".")) {
                String[] fields = fieldName.split("\\.");
                hbaseSchema.addColumn(fields[0], fields[1], dataType);
            }
        }
        return hbaseSchema;
    }
}
