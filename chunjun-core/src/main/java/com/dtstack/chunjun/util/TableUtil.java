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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.typeutil.ColumnRowDataTypeInfo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TableUtil {

    public static TypeInformation<RowData> getTypeInformation(
            List<FieldConfig> fieldList, RawTypeMapper converter, boolean useAbstractBaseColumn) {
        List<String> fieldName =
                fieldList.stream().map(FieldConfig::getName).collect(Collectors.toList());
        TypeConfig[] fieldTypes =
                fieldList.stream().map(FieldConfig::getType).toArray(TypeConfig[]::new);
        String[] fieldFormat =
                fieldList.stream().map(FieldConfig::getFormat).toArray(String[]::new);
        String[] fieldNames = fieldList.stream().map(FieldConfig::getName).toArray(String[]::new);
        if (fieldName.size() == 0
                || fieldName.get(0).equalsIgnoreCase(ConstantValue.STAR_SYMBOL)
                || Arrays.stream(fieldTypes).anyMatch(Objects::isNull)) {
            return new GenericTypeInfo<>(RowData.class);
        }
        DataType[] dataTypes = new DataType[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            dataTypes[i] = converter.apply(fieldTypes[i]);
        }
        return getTypeInformation(dataTypes, fieldNames, fieldFormat, useAbstractBaseColumn);
    }

    public static TypeInformation<RowData> getTypeInformation(
            DataType[] dataTypes, String[] fieldNames) {
        return getTypeInformation(dataTypes, fieldNames, new String[fieldNames.length], false);
    }

    public static TypeInformation<RowData> getTypeInformation(
            DataType[] dataTypes,
            String[] fieldNames,
            String[] fieldFormat,
            boolean useAbstractBaseColumn) {
        RowType rowType = getRowType(dataTypes, fieldNames, fieldFormat);

        if (useAbstractBaseColumn) {
            return ColumnRowDataTypeInfo.of(rowType);
        } else {
            return InternalTypeInfo.of(getRowType(dataTypes, fieldNames, fieldFormat));
        }
    }

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

    public static RowType createRowType(
            List<String> fieldNames, List<TypeConfig> types, RawTypeMapper converter) {
        List<DataType> dataTypeList = Lists.newLinkedList();
        for (int i = 0; i < types.size(); i++) {
            dataTypeList.add(i, converter.apply(types.get(i)));
        }
        return (RowType)
                ResolvedSchema.physical(fieldNames, dataTypeList)
                        .toPhysicalRowDataType()
                        .getLogicalType();
    }

    /**
     * only using in data sync/integration
     *
     * @param fields List<FieldConfig>, field information name, type etc.
     * @return
     */
    public static RowType createRowType(List<FieldConfig> fields, RawTypeMapper converter) {
        return (RowType)
                createTableSchema(fields, converter).toPhysicalRowDataType().getLogicalType();
    }

    /**
     * only using in data sync/integration
     *
     * @param fields List<FieldConfig>, field information name, type etc.
     * @return
     */
    public static ResolvedSchema createTableSchema(
            List<FieldConfig> fields, RawTypeMapper converter) {
        String[] fieldNames = fields.stream().map(FieldConfig::getName).toArray(String[]::new);
        TypeConfig[] fieldTypes =
                fields.stream().map(FieldConfig::getType).toArray(TypeConfig[]::new);
        DataType[] dataTypes = new DataType[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            dataTypes[i] = converter.apply(fieldTypes[i]);
        }
        return ResolvedSchema.physical(fieldNames, dataTypes);
    }
}
