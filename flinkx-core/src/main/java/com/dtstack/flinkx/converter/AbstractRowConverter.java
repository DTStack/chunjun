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

package com.dtstack.flinkx.converter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.sql.ResultSet;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Converter that is responsible to convert between JDBC object and Flink SQL internal data
 * structure {@link RowData}.
 */

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/10
 */
public abstract class AbstractRowConverter<SourceT, LookupT, SinkT, T> implements Serializable {

    private static final long serialVersionUID = -8805351737120663386L;
    protected RowType rowType;
    protected DeserializationConverter[] toInternalConverters;
    protected SerializationConverter[] toExternalConverters;
    protected LogicalType[] fieldTypes;

    public AbstractRowConverter() {
    }

    public AbstractRowConverter(RowType rowType) {
        this(rowType.getFieldCount());
        this.rowType = checkNotNull(rowType);
        this.fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
    }

    public AbstractRowConverter(int converterSize) {
        this.toInternalConverters = new DeserializationConverter[converterSize];
        this.toExternalConverters = new SerializationConverter[converterSize];
    }

    protected DeserializationConverter wrapIntoNullableInternalConverter(
            DeserializationConverter deserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return deserializationConverter.deserialize(val);
            }
        };
    }

    protected abstract SerializationConverter wrapIntoNullableExternalConverter(
            SerializationConverter serializationConverter, T type);

    /**
     * Convert data retrieved from {@link ResultSet} to internal {@link RowData}.
     *
     * @param input from JDBC
     */
    public abstract RowData toInternal(SourceT input) throws Exception;

    public abstract RowData toInternalLookup(LookupT input) throws Exception;

    /**
     * BinaryRowData
     *
     * @param rowData
     * @param output
     * @return
     */
    public abstract SinkT toExternal(RowData rowData, SinkT output) throws Exception;

    /** Runtime converter to convert field to {@link RowData} type object. */
    @FunctionalInterface
    protected interface DeserializationConverter<T> extends Serializable {
        Object deserialize(T field) throws Exception;
    }

    /**
     * 类型T一般是 Object，HBase这种特殊的就是byte[]
     *
     * @param <T>
     */
    @FunctionalInterface
    protected interface SerializationConverter<T> extends Serializable {
        void serialize(RowData rowData, int pos, T output) throws Exception;
    }

    /**
     * 将外部数据库类型转换为flink内部类型
     *
     * @param type
     * @return
     */
    protected abstract DeserializationConverter createInternalConverter(T type);

    /**
     * 将flink内部的数据类型转换为外部数据库系统类型
     *
     * @param type
     * @return
     */
    protected abstract SerializationConverter createExternalConverter(T type);
}
