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

package com.dtstack.chunjun.connector.kudu.converter;

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.ColumnRowData;
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

import org.apache.kudu.client.Operation;
import org.apache.kudu.client.RowResult;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.List;

public class KuduSqlConverter
        extends AbstractRowConverter<RowResult, RowResult, Operation, LogicalType> {

    private static final long serialVersionUID = -142881666054444897L;

    private final List<String> columnName;

    public KuduSqlConverter(RowType rowType, List<String> columnName) {
        super(rowType);
        this.columnName = columnName;
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(rowType.getTypeAt(i)), rowType.getTypeAt(i)));
        }
    }

    @Override
    protected ISerializationConverter<Operation> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, operation) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                operation.getRow().setNull(columnName.get(index));
            } else {
                serializationConverter.serialize(val, index, operation);
            }
        };
    }

    @Override
    public RowData toInternal(RowResult input) throws Exception {
        return deserializeInput(input);
    }

    @Override
    public RowData toInternalLookup(RowResult input) throws Exception {
        return deserializeInput(input);
    }

    private RowData deserializeInput(RowResult input) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = input.getObject(pos);
            genericRowData.setField(pos, toInternalConverters.get(pos).deserialize(field));
        }
        return genericRowData;
    }

    @Override
    public Operation toExternal(RowData rowData, Operation operation) throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, operation);
        }
        return operation;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot().name()) {
            case "BOOLEAN":
            case "DOUBLE":
            case "LONG":
            case "BIGINT":
            case "FLOAT":
            case "INTEGER":
            case "INT":
            case "SMALLINT":
                return val -> val;
            case "VARCHAR":
            case "STRING":
                return val -> StringData.fromString(val.toString());
            case "TINYINT":
                return val -> (byte) val;
            case "DECIMAL":
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case "DATE":
                return val -> (int) Date.valueOf(String.valueOf(val)).toLocalDate().toEpochDay();
            case "TIMESTAMP":
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
                return val -> TimestampData.fromEpochMillis(((java.util.Date) val).getTime());
            case "BINARY":
                return val -> (byte[]) val;
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<Operation> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot().name()) {
            case "BOOLEAN":
            case "BOOL":
                return (val, index, operation) ->
                        operation.getRow().addBoolean(columnName.get(index), val.getBoolean(index));
            case "TINYINT":
            case "CHAR":
            case "INT8":
                return (val, index, operation) ->
                        operation.getRow().addByte(columnName.get(index), val.getByte(index));
            case "INT16":
            case "SMALLINT":
                return (val, index, operation) ->
                        operation.getRow().addShort(columnName.get(index), val.getShort(index));
            case "INTEGER":
            case "INT":
            case "INT32":
                return (val, index, operation) ->
                        operation.getRow().addInt(columnName.get(index), val.getInt(index));
            case "BIGINT":
            case "INT64":
                return (val, index, operator) ->
                        operator.getRow().addLong(columnName.get(index), val.getLong(index));
            case "FLOAT":
                return (val, index, operation) ->
                        operation.getRow().addFloat(columnName.get(index), val.getFloat(index));
            case "DOUBLE":
                return (val, index, operation) ->
                        operation.getRow().addDouble(columnName.get(index), val.getDouble(index));
            case "BINARY":
                return (val, index, operation) ->
                        operation.getRow().addBinary(columnName.get(index), val.getBinary(index));
            case "DECIMAL":
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index, operation) ->
                        operation
                                .getRow()
                                .addDecimal(
                                        index,
                                        val.getDecimal(index, decimalPrecision, decimalScale)
                                                .toBigDecimal());
            case "VARCHAR":
                return (val, index, operation) ->
                        operation
                                .getRow()
                                .addString(columnName.get(index), val.getString(index).toString());
            case "DATE":
                return (val, index, operation) ->
                        operation
                                .getRow()
                                .addDate(
                                        columnName.get(index),
                                        Date.valueOf(
                                                LocalDate.ofEpochDay(
                                                        ((ColumnRowData) val)
                                                                .getField(index)
                                                                .asInt())));

            case "TIMESTAMP":
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
                return (val, index, operation) ->
                        operation
                                .getRow()
                                .addTimestamp(
                                        columnName.get(index),
                                        new Timestamp(
                                                ((TimestampData)
                                                                ((GenericRowData) val)
                                                                        .getField(index))
                                                        .getMillisecond()));
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
