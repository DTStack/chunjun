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

}
