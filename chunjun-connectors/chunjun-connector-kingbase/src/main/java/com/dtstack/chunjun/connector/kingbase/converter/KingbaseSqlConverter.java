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
package com.dtstack.chunjun.connector.kingbase.converter;

import com.dtstack.chunjun.connector.jdbc.converter.JdbcSqlConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.buffer.impl.BufferImpl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class KingbaseSqlConverter extends JdbcSqlConverter {

    private static final long serialVersionUID = 2L;

    public KingbaseSqlConverter(RowType rowType) {
        super(rowType);
    }

    /**
     * override reason: tinying type in KingBase is byte type, couldn't case int.
     *
     * @param type
     * @return
     */
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
                return val -> val;
            case TINYINT:
                return val -> ((Byte) val);
            case SMALLINT:
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val -> {
                    if (val instanceof LocalDateTime) {
                        long time = Timestamp.valueOf((LocalDateTime) val).getTime();
                        return Long.valueOf(((time + LOCAL_TZ.getOffset(time)) / 86400000L))
                                .intValue();
                    } else if (val instanceof Timestamp) {
                        return Long.valueOf(((Timestamp) val).getTime() / 1000).intValue();
                    } else {
                        return (int)
                                ((Date.valueOf(String.valueOf(val))).toLocalDate().toEpochDay());
                    }
                };
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int)
                                ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    if (val instanceof String) {
                        return TimestampData.fromTimestamp(Timestamp.valueOf((String) val));
                    } else if (val instanceof LocalDateTime) {
                        return TimestampData.fromTimestamp(Timestamp.valueOf((LocalDateTime) val));
                    } else {
                        return TimestampData.fromTimestamp(((Timestamp) val));
                    }
                };
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> {
                    if (val instanceof BufferImpl) {
                        return ((BufferImpl) val).getBytes();
                    }
                    return (byte[]) val;
                };
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
