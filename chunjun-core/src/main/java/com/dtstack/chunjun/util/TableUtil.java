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
package com.dtstack.chunjun.util;

import com.dtstack.chunjun.conf.FieldConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.typeutil.ColumnRowDataTypeInfo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TableUtil {

    /**
     * 获取TypeInformation
     *
     * @param fieldList 任务参数实体类
     * @return TypeInformation
     */
    public static TypeInformation<RowData> getTypeInformation(
            List<FieldConfig> fieldList,
            RawTypeConverter converter,
            boolean useAbstractBaseColumn) {
        List<String> fieldName =
                fieldList.stream().map(FieldConfig::getName).collect(Collectors.toList());
        String[] fieldTypes = fieldList.stream().map(FieldConfig::getType).toArray(String[]::new);
        String[] fieldFormat =
                fieldList.stream().map(FieldConfig::getFormat).toArray(String[]::new);
        String[] fieldNames = fieldList.stream().map(FieldConfig::getName).toArray(String[]::new);
        if (fieldName.size() == 0
                || fieldName.get(0).equalsIgnoreCase(ConstantValue.STAR_SYMBOL)
                || Arrays.stream(fieldTypes).anyMatch(Objects::isNull)) {
            return new GenericTypeInfo<>(RowData.class);
        }
        TableSchema.Builder builder = TableSchema.builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            DataType dataType = converter.apply(fieldTypes[i]);
            builder.add(TableColumn.physical(fieldNames[i], dataType));
        }
        DataType[] dataTypes =
                builder.build().toRowDataType().getChildren().toArray(new DataType[] {});

        return getTypeInformation(dataTypes, fieldNames, fieldFormat, useAbstractBaseColumn);
    }

    /**
     * 获取TypeInformation
     *
     * @param dataTypes
     * @param fieldNames
     * @return
     */
    public static TypeInformation<RowData> getTypeInformation(
            DataType[] dataTypes, String[] fieldNames) {
        return getTypeInformation(dataTypes, fieldNames, new String[fieldNames.length], false);
    }

    /**
     * 获取TypeInformation
     *
     * @param dataTypes
     * @param fieldNames
     * @return
     */
    public static TypeInformation<RowData> getTypeInformation(
            DataType[] dataTypes,
            String[] fieldNames,
            String[] fieldFormat,
            boolean useAbstractBaseColumn) {
        RowType rowType = getRowType(dataTypes, fieldNames, fieldFormat);

        if (useAbstractBaseColumn) {
            if (useGenericTypeInfo(rowType)) {
                return new GenericTypeInfo<>(RowData.class);
            }
            return ColumnRowDataTypeInfo.of(rowType);
        } else {
            return InternalTypeInfo.of(getRowType(dataTypes, fieldNames, fieldFormat));
        }
    }

    public static boolean useGenericTypeInfo(RowType rowType) {
        return rowType.getChildren().stream()
                .map(LogicalType::getTypeRoot)
                .anyMatch(logicalTypeRoot -> logicalTypeRoot == LogicalTypeRoot.ARRAY);
    }

    /**
     * 获取RowType
     *
     * @param dataTypes
     * @param fieldNames
     * @return
     */
    public static RowType getRowType(
            DataType[] dataTypes, String[] fieldNames, String[] formatField) {
        List<RowType.RowField> rowFieldList = new ArrayList<>(dataTypes.length);
        if (formatField == null || formatField.length == 0) {
            for (int i = 0; i < dataTypes.length; i++) {
                rowFieldList.add(
                        new RowType.RowField(fieldNames[i], dataTypes[i].getLogicalType()));
            }
        } else {
            for (int i = 0; i < dataTypes.length; i++) {
                rowFieldList.add(
                        new RowType.RowField(
                                fieldNames[i], dataTypes[i].getLogicalType(), formatField[i]));
            }
        }

        return new RowType(rowFieldList);
    }

    /**
     * only using in data sync/integration
     *
     * @param fieldNames field Names
     * @param types field types
     * @return
     */
    public static RowType createRowType(
            List<String> fieldNames, List<String> types, RawTypeConverter converter) {
        TableSchema.Builder builder = TableSchema.builder();
        for (int i = 0; i < types.size(); i++) {
            DataType dataType = converter.apply(types.get(i));
            builder.add(TableColumn.physical(fieldNames.get(i), dataType));
        }
        return (RowType) builder.build().toRowDataType().getLogicalType();
    }

    /**
     * only using in data sync/integration
     *
     * @param fields List<FieldConf>, field information name, type etc.
     * @return
     */
    public static RowType createRowType(List<FieldConfig> fields, RawTypeConverter converter) {
        return (RowType) createTableSchema(fields, converter).toRowDataType().getLogicalType();
    }

    /**
     * only using in data sync/integration
     *
     * @param fields List<FieldConf>, field information name, type etc.
     * @return
     */
    public static TableSchema createTableSchema(
            List<FieldConfig> fields, RawTypeConverter converter) {
        String[] fieldNames = fields.stream().map(FieldConfig::getName).toArray(String[]::new);
        String[] fieldTypes = fields.stream().map(FieldConfig::getType).toArray(String[]::new);
        TableSchema.Builder builder = TableSchema.builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            DataType dataType = converter.apply(fieldTypes[i]);
            builder.add(TableColumn.physical(fieldNames[i], dataType));
        }
        return builder.build();
    }
}
