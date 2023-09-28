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

package com.dtstack.chunjun.connector.mongodb.converter;

import com.dtstack.chunjun.converter.AbstractRowConverter;

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

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

public final class MongodbSqlConverter
        extends AbstractRowConverter<Document, Document, Document, LogicalType> {

    private static final long serialVersionUID = -2683857781343809969L;

    private final List<MongoDeserializationConverter> toInternalConverters;
    private final List<MongoSerializationConverter> toExternalConverters;
    private final String[] fieldNames;

    public MongodbSqlConverter(RowType rowType, String[] fieldNames) {
        super(rowType);
        this.fieldNames = fieldNames;
        toInternalConverters = new ArrayList<>();
        toExternalConverters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createMongoInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableMongodbExternalConverter(
                            createMongodbExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
    }

    private MongoDeserializationConverter wrapIntoNullableInternalConverter(
            MongoDeserializationConverter deserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return deserializationConverter.deserialize(val);
            }
        };
    }

    private MongoSerializationConverter wrapIntoNullableMongodbExternalConverter(
            MongoSerializationConverter serializationConverter, LogicalType type) {
        return (val, index, key, document) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                document.append(key, null);
            } else {
                serializationConverter.serialize(val, index, key, document);
            }
        };
    }

    @Override
    public RowData toInternal(Document document) {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = document.get(fieldNames[pos]);
            if (field instanceof ObjectId) {
                field = field.toString();
            }
            genericRowData.setField(pos, toInternalConverters.get(pos).deserialize(field));
        }
        return genericRowData;
    }

    @Override
    public RowData toInternalLookup(Document document) {
        return toInternal(document);
    }

    @Override
    public Document toExternal(RowData rowData, Document document) {
        for (int pos = 0; pos < fieldTypes.length; pos++) {
            toExternalConverters.get(pos).serialize(rowData, pos, fieldNames[pos], document);
        }
        return document;
    }

    private MongoDeserializationConverter createMongoInternalConverter(LogicalType type) {
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
                return val -> val;
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return val ->
                        DecimalData.fromBigDecimal(
                                ((Decimal128) val).bigDecimalValue(), precision, scale);
            case DATE:
                return val -> {
                    Date date = new Date(((java.util.Date) val).getTime());
                    return (int) date.toLocalDate().toEpochDay();
                };

            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int)
                                ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    if (val instanceof java.util.Date) {
                        return TimestampData.fromEpochMillis(((java.util.Date) val).getTime());
                    }
                    return TimestampData.fromTimestamp((Timestamp) val);
                };
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> ((Binary) val).getData();
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private MongoSerializationConverter createMongodbExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, key, document) -> document.append(key, val.getBoolean(index));
            case TINYINT:
                return (val, index, key, document) -> document.append(key, val.getByte(index));
            case SMALLINT:
                return (val, index, key, document) -> document.append(key, val.getShort(index));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, index, key, document) -> document.append(key, val.getInt(index));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, index, key, document) -> document.append(key, val.getLong(index));
            case FLOAT:
                return (val, index, key, document) -> document.append(key, val.getFloat(index));
            case DOUBLE:
                return (val, index, key, document) -> document.append(key, val.getDouble(index));
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (val, index, key, document) ->
                        document.append(key, val.getString(index).toString());
            case BINARY:
            case VARBINARY:
                return (val, index, key, document) -> document.append(key, val.getBinary(index));
            case DATE:
                return (val, index, key, document) ->
                        document.append(key, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));

            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, key, document) ->
                        document.append(
                                key,
                                Time.valueOf(
                                        LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));

            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index, key, document) ->
                        document.append(
                                key, val.getTimestamp(index, timestampPrecision).toTimestamp());

            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index, key, document) ->
                        document.append(
                                key,
                                val.getDecimal(index, decimalPrecision, decimalScale)
                                        .toBigDecimal());

            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
