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

package com.dtstack.flinkx.table;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Converter that is responsible to convert between JDBC object and Flink SQL internal data
 * structure {@link RowData}.
 */

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/10
 **/
public abstract class AbstractRowDataConverter<SourceT, SinkT> implements Serializable {

    protected final RowType rowType;
    protected final DeserializationConverter[] toInternalConverters;
    protected final SerializationConverter[] toExternalConverters;
    protected final LogicalType[] fieldTypes;

    public AbstractRowDataConverter(RowType rowType) {
        this.rowType = checkNotNull(rowType);
        this.fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        this.toInternalConverters = new DeserializationConverter[rowType.getFieldCount()];
        this.toExternalConverters = new SerializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
            toExternalConverters[i] = createNullableExternalConverter(fieldTypes[i]);
        }
    }

    /**
     * Convert data retrieved from {@link ResultSet} to internal {@link RowData}.
     *
     * @param input from JDBC
     */
    public abstract RowData toInternal(SourceT input);

    /**
     * Convert data retrieved from Flink internal RowData to JDBC Object.
     *
     * @param rowData The given internal {@link RowData}.
     *
     * @return The filled statement.
     */
    public abstract SinkT toExternal(RowData rowData);


    /** Runtime converter to convert field to {@link RowData} type object. */
    @FunctionalInterface
    interface DeserializationConverter<T> extends Serializable {
        Object deserialize(T field) throws SQLException;
    }

    /**
     * 类型T一般是 Object，HBase这种特殊的就是byte[]
     * @param <T>
     */
    @FunctionalInterface
    interface SerializationConverter<T> extends Serializable {
        T serialize(RowData rowData, int pos);
    }

    /**
     * Create a nullable runtime {@link DeserializationConverter} from given {@link
     * LogicalType}.
     */
    protected DeserializationConverter createNullableInternalConverter(LogicalType type) {
        return wrapIntoNullableInternalConverter(createInternalConverter(type));
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

    /**
     * 先调用这个
     *
     * @param type
     *
     * @return
     */
    protected abstract DeserializationConverter createInternalConverter(LogicalType type);

    /** Create a nullable JDBC f{@link SerializationConverter} from given sql type. */
    protected SerializationConverter createNullableExternalConverter(LogicalType type) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type), type);
    }

    protected abstract SerializationConverter wrapIntoNullableExternalConverter(
            SerializationConverter serializationConverter, LogicalType type);

    /**
     * 先调用这个
     *
     * @param type
     *
     * @return
     */
    protected abstract SerializationConverter createExternalConverter(LogicalType type);
}
