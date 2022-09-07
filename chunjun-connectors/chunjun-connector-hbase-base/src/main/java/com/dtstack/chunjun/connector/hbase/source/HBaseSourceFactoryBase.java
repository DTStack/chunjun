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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.OperatorConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase.converter.HBaseColumnConverter;
import com.dtstack.chunjun.connector.hbase.converter.HBaseRawTypeConverter;
import com.dtstack.chunjun.connector.hbase.converter.HBaseRowConverter;
import com.dtstack.chunjun.connector.hbase.util.ScanBuilder;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
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

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HBaseSourceFactoryBase extends SourceFactory {

    protected final HBaseConf hBaseConf;

    public HBaseSourceFactoryBase(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        OperatorConf reader = syncConf.getReader();
        hBaseConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(reader.getParameter()), HBaseConf.class);
        super.initCommonConf(hBaseConf);
        hBaseConf.setColumnMetaInfos(reader.getFieldList());
        Map<String, Object> range =
                (Map<String, Object>) syncConf.getReader().getParameter().get("range");
        if (range != null) {
            if (range.get("startRowkey") != null
                    && StringUtils.isNotBlank(range.get("startRowkey").toString())) {
                hBaseConf.setStartRowkey(range.get("startRowkey").toString());
            }
            if (range.get("endRowkey") != null
                    && StringUtils.isNotBlank(range.get("endRowkey").toString())) {
                hBaseConf.setEndRowkey(range.get("endRowkey").toString());
            }
            if (range.get("isBinaryRowkey") != null) {
                hBaseConf.setBinaryRowkey((Boolean) range.get("isBinaryRowkey"));
            }
        }
        typeInformation =
                TableUtil.getTypeInformation(
                        fieldList.stream()
                                .peek(
                                        fieldConf ->
                                                fieldConf.setName(
                                                        fieldConf.getName().replace(":", ".")))
                                .collect(Collectors.toList()),
                        getRawTypeConverter(),
                        useAbstractBaseColumn);
        if (hBaseConf.getNullStringLiteral() == null
                || "".equals(hBaseConf.getNullStringLiteral().trim())) {
            hBaseConf.setNullStringLiteral("null");
        }
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return HBaseRawTypeConverter.INSTANCE;
    }

    @Override
    @SuppressWarnings("all")
    public DataStream<RowData> createSource() {
        List<FieldConf> fieldConfList = hBaseConf.getColumnMetaInfos();

        AbstractRowConverter rowConverter;
        ScanBuilder scanBuilder;
        if (useAbstractBaseColumn) {
            final RowType rowType =
                    TableUtil.createRowType(hBaseConf.getColumn(), getRawTypeConverter());
            rowConverter = new HBaseColumnConverter(hBaseConf, rowType);
            scanBuilder = ScanBuilder.forSync(fieldConfList);
        } else {
            String nullStringLiteral = hBaseConf.getNullStringLiteral();
            Set<String> columnSet = new LinkedHashSet<>();
            for (FieldConf fieldConf : fieldConfList) {
                String fieldName = fieldConf.getName();
                if ("rowkey".equalsIgnoreCase(fieldName)) {
                    columnSet.add(fieldName);
                } else if (fieldName.contains(".")) {
                    String[] familyQualifier = fieldName.split("\\.");
                    columnSet.add(familyQualifier[0]);
                }
            }
            this.typeInformation = buildType(columnSet);
            HBaseTableSchema hBaseTableSchema =
                    buildHBaseTableSchema(hBaseConf.getTable(), fieldConfList);
            syncConf.getReader().setFieldNameList(new ArrayList<>(columnSet));
            final RowType rowType =
                    TableUtil.createRowType(hBaseConf.getColumn(), getRawTypeConverter());
            rowConverter = new HBaseRowConverter(hBaseTableSchema, nullStringLiteral);
            scanBuilder = ScanBuilder.forSql(hBaseTableSchema);
        }

        HBaseInputFormatBuilder builder =
                HBaseInputFormatBuilder.newBuild(hBaseConf.getTable(), scanBuilder);

        builder.setConfig(hBaseConf);
        builder.setColumnMetaInfos(hBaseConf.getColumnMetaInfos());
        builder.setHbaseConfig(hBaseConf.getHbaseConfig());
        builder.setEndRowKey(hBaseConf.getEndRowkey());
        builder.setIsBinaryRowkey(hBaseConf.isBinaryRowkey());
        builder.setScanCacheSize(hBaseConf.getScanCacheSize());
        builder.setStartRowKey(hBaseConf.getStartRowkey());

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
        for (FieldConf fieldConf : fieldList) {
            String fieldName = fieldConf.getName();
            if ("rowkey".equalsIgnoreCase(fieldConf.getName())) {
                DataType dataType = this.getRawTypeConverter().apply(fieldConf.getType());
                builder.add(TableColumn.physical(fieldName, dataType));
            } else if (fieldName.contains(".")) {
                String[] familyQualifier = fieldName.split("\\.");
                Map<String, DataType> qualifier = family.get(familyQualifier[0]);
                qualifier.put(
                        familyQualifier[1], this.getRawTypeConverter().apply(fieldConf.getType()));
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

    HBaseTableSchema buildHBaseTableSchema(String tableName, List<FieldConf> fieldConfList) {
        HBaseTableSchema hbaseSchema = new HBaseTableSchema();
        hbaseSchema.setTableName(tableName);
        RawTypeConverter rawTypeConverter = getRawTypeConverter();
        for (FieldConf fieldConf : fieldConfList) {
            String fieldName = fieldConf.getName();
            DataType dataType = rawTypeConverter.apply(fieldConf.getType());
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
