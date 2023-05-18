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

package com.dtstack.chunjun.connector.doris.converter;

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

public class DorisHttpSqlConverter
        extends AbstractRowConverter<RowData, RowData, String[], LogicalType> {

    private static final String NULL_VALUE = "\\N";

    private static final long serialVersionUID = -2636292632781799617L;

    public DorisHttpSqlConverter(RowType rowType) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
    }

    @Override
    public RowData toInternal(RowData input) {
        return null;
    }

    @Override
    public String[] toExternal(RowData rowData, String[] joiner) throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, joiner);
        }
        return joiner;
    }

    @Override
    protected ISerializationConverter<String[]> wrapIntoNullableExternalConverter(
            ISerializationConverter<String[]> ISerializationConverter, LogicalType type) {
        return ((rowData, index, joiner) -> {
            if (rowData == null
                    || rowData.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                joiner[index] = NULL_VALUE;
            } else {
                ISerializationConverter.serialize(rowData, index, joiner);
            }
        });
    }

    @Override
    protected ISerializationConverter<String[]> createExternalConverter(LogicalType type) {
        return (rowData, index, joiner) -> {
            Object value = ((GenericRowData) rowData).getField(index);
            joiner[index] = "".equals(value.toString()) ? NULL_VALUE : value.toString();
        };
    }
}
