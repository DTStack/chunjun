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
package com.dtstack.chunjun.connector.hdfs.converter;

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

public class HdfsOrcSqlConverter
        extends AbstractRowConverter<RowData, RowData, Object[], LogicalType> {

    private static final long serialVersionUID = 6632938157518455020L;

    public HdfsOrcSqlConverter(RowType rowType) {
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
    @SuppressWarnings("unchecked")
    public RowData toInternal(RowData input) throws Exception {
        GenericRowData row = new GenericRowData(input.getArity());
        if (input instanceof GenericRowData) {
            GenericRowData genericRowData = (GenericRowData) input;
            for (int i = 0; i < input.getArity(); i++) {
                row.setField(
                        i, toInternalConverters.get(i).deserialize(genericRowData.getField(i)));
            }
        } else {
            throw new ChunJunRuntimeException(
                    "Error RowData type, RowData:["
                            + input
                            + "] should be instance of GenericRowData.");
        }
        return row;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object[] toExternal(RowData rowData, Object[] data) throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, data);
        }
        return data;
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new ChunJunRuntimeException("HDFS Connector doesn't support Lookup Table Function.");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<Object[]> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (rowData, index, data) -> {
            if (rowData == null
                    || rowData.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                data[index] = null;
            } else {
                serializationConverter.serialize(rowData, index, data);
            }
        };
    }

    @Override
    @SuppressWarnings("all")
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return (IDeserializationConverter<Boolean, Boolean>) val -> val;
            case TINYINT:
                return (IDeserializationConverter<Byte, Byte>) val -> val;
            case SMALLINT:
                return (IDeserializationConverter<Short, Short>) val -> val;
            case INTEGER:
                return (IDeserializationConverter<Integer, Integer>) val -> val;
            case BIGINT:
                return (IDeserializationConverter<Long, Long>) val -> val;
            case DATE:
                return (IDeserializationConverter<Date, Integer>)
                        val -> (int) val.toLocalDate().toEpochDay();
            case FLOAT:
                return (IDeserializationConverter<Float, Float>) val -> val;
            case DOUBLE:
                return (IDeserializationConverter<Double, Double>) val -> val;
            case CHAR:
            case VARCHAR:
                return (IDeserializationConverter<String, StringData>) StringData::fromString;
            case DECIMAL:
                return (IDeserializationConverter<BigDecimal, DecimalData>)
                        val -> DecimalData.fromBigDecimal(val, val.precision(), val.scale());
            case BINARY:
            case VARBINARY:
                return (IDeserializationConverter<byte[], byte[]>) val -> val;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Timestamp, TimestampData>)
                        TimestampData::fromTimestamp;
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    @Override
    protected ISerializationConverter<Object[]> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (rowData, index, data) -> data[index] = null;
            case BOOLEAN:
                return (rowData, index, data) -> data[index] = rowData.getBoolean(index);
            case TINYINT:
                return (rowData, index, data) -> data[index] = rowData.getByte(index);
            case SMALLINT:
                return (rowData, index, data) -> data[index] = rowData.getShort(index);
            case INTEGER:
                return (rowData, index, data) -> data[index] = rowData.getInt(index);
            case BIGINT:
                return (rowData, index, data) -> data[index] = rowData.getLong(index);
            case DATE:
                return (rowData, index, data) -> {
                    data[index] = Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(index)));
                };
            case FLOAT:
                return (rowData, index, data) -> data[index] = rowData.getFloat(index);
            case DOUBLE:
                return (rowData, index, data) -> data[index] = rowData.getDouble(index);
            case CHAR:
            case VARCHAR:
                return (rowData, index, data) -> data[index] = rowData.getString(index).toString();
            case DECIMAL:
                return (rowData, index, data) -> {
                    int precision = ((DecimalType) type).getPrecision();
                    int scale = ((DecimalType) type).getScale();
                    HiveDecimal hiveDecimal =
                            HiveDecimal.create(
                                    rowData.getDecimal(index, precision, scale).toBigDecimal());
                    hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, precision, scale);
                    data[index] = new HiveDecimalWritable(hiveDecimal);
                };
            case BINARY:
            case VARBINARY:
                return (rowData, index, data) ->
                        data[index] = new BytesWritable(rowData.getBinary(index));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (rowData, index, data) ->
                        data[index] =
                                rowData.getTimestamp(index, ((TimestampType) type).getPrecision())
                                        .toTimestamp();
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
