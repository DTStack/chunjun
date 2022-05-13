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
package com.dtstack.flinkx.util;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.converter.RawTypeConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Date: 2021/04/07 Company: www.dtstack.com
 *
 * @author tudou
 */
public class TableUtil {

    /**
     * 获取TypeInformation
     *
     * @param fieldList 任务参数实体类
     * @return TypeInformation
     */
    public static TypeInformation<RowData> getTypeInformation(
            List<FieldConf> fieldList, RawTypeConverter converter) {
        List<String> fieldName =
                fieldList.stream().map(FieldConf::getName).collect(Collectors.toList());
        if (fieldName.size() == 0) {
            return new GenericTypeInfo<>(RowData.class);
        }

        String[] fieldNames = fieldList.stream().map(FieldConf::getName).toArray(String[]::new);
        String[] fieldTypes = fieldList.stream().map(FieldConf::getType).toArray(String[]::new);
        TableSchema.Builder builder = TableSchema.builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            DataType dataType = converter.apply(fieldTypes[i]);
            builder.add(TableColumn.physical(fieldNames[i], dataType));
        }
        DataType[] dataTypes =
                builder.build().toRowDataType().getChildren().toArray(new DataType[] {});

        return getTypeInformation(dataTypes, fieldNames);
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
        RowType type =
                RowType.of(
                        Arrays.stream(dataTypes)
                                .map(DataType::getLogicalType)
                                .toArray(LogicalType[]::new),
                        fieldNames);
        // TODO 1.15 包的位置改变了 @wujuan
        return InternalTypeInfo.of(getRowType(dataTypes, fieldNames));
    }

    /**
     * 获取RowType
     *
     * @param dataTypes
     * @param fieldNames
     * @return
     */
    public static RowType getRowType(DataType[] dataTypes, String[] fieldNames) {
        return RowType.of(
                Arrays.stream(dataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new),
                fieldNames);
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
    public static RowType createRowType(List<FieldConf> fields, RawTypeConverter converter) {
        return (RowType) createTableSchema(fields, converter).toRowDataType().getLogicalType();
    }

    /**
     * only using in data sync/integration
     *
     * @param fields List<FieldConf>, field information name, type etc.
     * @return
     */
    public static TableSchema createTableSchema(
            List<FieldConf> fields, RawTypeConverter converter) {
        String[] fieldNames = fields.stream().map(FieldConf::getName).toArray(String[]::new);
        String[] fieldTypes = fields.stream().map(FieldConf::getType).toArray(String[]::new);
        TableSchema.Builder builder = TableSchema.builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            DataType dataType = converter.apply(fieldTypes[i]);
            builder.add(TableColumn.physical(fieldNames[i], dataType));
        }
        return builder.build();
    }
}
