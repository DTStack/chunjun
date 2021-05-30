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

package com.dtstack.flinkx.connector.oracle.converter;

import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import com.dtstack.flinkx.connector.jdbc.converter.JdbcRowConverter;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import oracle.sql.TIMESTAMP;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleRowConverter
        extends JdbcRowConverter {

    private static final long serialVersionUID = 1L;

    public OracleRowConverter(RowType rowType) {
        super(rowType);
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
                return val -> val;
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case INTEGER:
                return val -> val;
            case BIGINT:
                return val -> val;
            case DECIMAL:
                return val -> {
                    if(val instanceof BigInteger){
                        // sometime decimal will return cast to BigInteger in lookup
                        BigDecimal bigDecimal = new BigDecimal((BigInteger) val);
                        return DecimalData.fromBigDecimal(bigDecimal,
                                bigDecimal.precision(),
                                bigDecimal.scale());
                    }else{
                        return DecimalData.fromBigDecimal((BigDecimal) val,
                                ((BigDecimal) val).precision(),
                                (((BigDecimal) val).scale()));
                    }
                };
            case DATE:
                return val -> ((Timestamp) val).getNanos();
            case TIME_WITHOUT_TIME_ZONE:
                return val -> (int) ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    if(val instanceof String){
                        // oracle.sql.TIMESTAMP will return cast to String in lookup
                        return TimestampData.fromTimestamp(Timestamp.valueOf((String) val));
                    }else {
                        try {
                            return TimestampData.fromTimestamp(((TIMESTAMP) val).timestampValue());
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                            throw new UnsupportedTypeException(
                                    "Unsupported type:" + type + ",value:" + val);
                        }
                    }
                };
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> (byte[]) val;
            default:
                throw new UnsupportedTypeException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) ->
                        statement.setBoolean(index, val.getBoolean(index));
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index, val.getByte(index));
            case SMALLINT:
                return (val, index, statement) -> statement.setShort(index, val.getShort(index));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, index, statement) -> statement.setInt(index, val.getInt(index));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, index, statement) -> statement.setLong(index, val.getLong(index));
            case FLOAT:
                return (val, index, statement) -> statement.setFloat(index, val.getFloat(index));
            case DOUBLE:
                return (val, index, statement) -> statement.setDouble(index, val.getDouble(index));
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (val, index, statement) ->
                        statement.setString(index, val.getString(index).toString());
            case BINARY:
            case VARBINARY:
                return (val, index, statement) -> statement.setBytes(index, val.getBinary(index));
            case DATE:
                return (val, index, statement) ->
                        statement.setDate(
                                index, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTime(
                                index,
                                Time.valueOf(
                                        LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, val.getTimestamp(index, timestampPrecision).toTimestamp());
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index, statement) ->
                        statement.setBigDecimal(
                                index,
                                val.getDecimal(index, decimalPrecision, decimalScale)
                                        .toBigDecimal());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
