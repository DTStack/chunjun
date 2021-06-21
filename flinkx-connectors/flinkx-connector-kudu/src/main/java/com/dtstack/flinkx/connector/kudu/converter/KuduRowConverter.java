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

package com.dtstack.flinkx.connector.kudu.converter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.RowResult;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;

/**
 * @author tiezhu
 * @since 2021/6/10 星期四
 */
public class KuduRowConverter
        extends AbstractRowConverter<RowResult, RowResult, Operation, LogicalType> {

    private static final long serialVersionUID = 1L;

    public KuduRowConverter(RowType rowType) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters[i] =
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i)));
            toExternalConverters[i] =
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]);
        }
    }

    @Override
    protected ISerializationConverter<Operation> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, operation) -> {
            if (((ColumnRowData) val).getField(index) == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                operation.getRow().setNull(index);
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
    public RowData toInternalLookup(RowResult input) {
        return deserializeInput(input);
    }

    @SuppressWarnings("unchecked")
   private RowData deserializeInput(RowResult input) {
       GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
       for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
           Object field = input.getObject(pos);
           genericRowData.setField(pos, toInternalConverters[pos].deserialize(field));
       }
       return genericRowData;
   }

    @Override
    public Operation toExternal(RowData rowData, Operation operation) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, operation);
        }
        return operation;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case INTEGER:
            case BIGINT:
            case SMALLINT:
                return val -> val;
            case TINYINT:
                return val -> (byte) val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val ->
                        (int) ((Date.valueOf(String.valueOf(val))).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int)
                                ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp((Timestamp) val);
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> (byte[]) val;
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    @Override
    protected ISerializationConverter<Operation> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot().name()) {
            case "BOOLEAN":
                return (val, index, operation) ->
                        operation.getRow().addBoolean(index, val.getBoolean(index));
            case "TINYINT":
                return (val, index, operation) ->
                        operation.getRow().addByte(index, val.getByte(index));
            case "SMALLINT":
            case "INTEGER":
                return (val, index, operation) ->
                        operation.getRow().addInt(index, val.getInt(index));
            case "FLOAT":
                return (val, index, operation) ->
                        operation.getRow().addFloat(index, val.getFloat(index));
            case "DOUBLE":
                return (val, index, operation) ->
                        operation.getRow().addDouble(index, val.getDouble(index));

            case "BIGINT":
                return (val, index, operation) ->
                        operation.getRow().addLong(index, val.getLong(index));
            case "BINARY":
                return (val, index, operation) ->
                        operation.getRow().addBinary(index, val.getBinary(index));
            case "DECIMAL":
                return (val, index, operation) ->
                        operation
                                .getRow()
                                .addDecimal(
                                        index,
                                        ((ColumnRowData) val).getField(index).asBigDecimal());
            case "CHAR":
            case "VARCHAR":
                return (val, index, operation) ->
                        operation.getRow().addString(index, val.getString(index).toString());
            case "DATE":
                return (val, index, operation) ->
                        operation
                                .getRow()
                                .addDate(
                                        index,
                                        Date.valueOf(
                                                LocalDate.ofEpochDay(
                                                        ((ColumnRowData) val)
                                                                .getField(index)
                                                                .asInt())));

            case "TIMESTAMP":
                return (val, index, operation) ->
                        operation
                                .getRow()
                                .addTimestamp(
                                        index, ((ColumnRowData) val).getField(index).asTimestamp());
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
