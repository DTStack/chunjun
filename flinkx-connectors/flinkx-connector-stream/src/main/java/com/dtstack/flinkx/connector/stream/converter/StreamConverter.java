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

package com.dtstack.flinkx.connector.stream.converter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author chuixue
 * @create 2021-04-10 11:39
 * @description 数据类型转换器
 **/
public class StreamConverter {

    protected final RowType rowType;
    protected final StreamDeserializationConverter[] toInternalConverters;
    protected final LogicalType[] fieldTypes;

    public StreamConverter(RowType rowType) {
        this.rowType = checkNotNull(rowType);
        this.fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        this.toInternalConverters = new StreamDeserializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
        }
    }

    public RowData toInternal(ResultSet resultSet) throws SQLException {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = resultSet.getObject(pos + 1);
            genericRowData.setField(pos, toInternalConverters[pos].deserialize(field));
        }
        return genericRowData;
    }

    /** Runtime converter to convert JDBC field to {@link RowData} type object. */
    @FunctionalInterface
    interface StreamDeserializationConverter extends Serializable {
        /**
         * Convert a jdbc field object of {@link ResultSet} to the internal data structure object.
         *
         * @param jdbcField
         */
        Object deserialize(Object jdbcField) throws SQLException;
    }

    /**
     * Create a nullable runtime {@link StreamDeserializationConverter} from given {@link
     * LogicalType}.
     */
    protected StreamDeserializationConverter createNullableInternalConverter(LogicalType type) {
        return wrapIntoNullableInternalConverter(createInternalConverter(type));
    }

    protected StreamDeserializationConverter wrapIntoNullableInternalConverter(
            StreamDeserializationConverter streamDeserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return streamDeserializationConverter.deserialize(val);
            }
        };
    }

    protected StreamDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return val -> val;
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case INTEGER:
                return val -> val;
            case BIGINT:
                return val -> val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val -> (int) (((Date) val).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val -> (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp((Timestamp) val);
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString((String) val);
            case BINARY:
            case VARBINARY:
                return val -> (byte[]) val;
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
