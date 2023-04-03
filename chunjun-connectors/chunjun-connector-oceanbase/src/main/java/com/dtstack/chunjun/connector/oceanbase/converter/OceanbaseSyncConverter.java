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

package com.dtstack.chunjun.connector.oceanbase.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcSyncConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class OceanbaseSyncConverter extends JdbcSyncConverter {
    public OceanbaseSyncConverter(RowType rowType, CommonConfig commonConfig) {
        super(rowType, commonConfig);
    }

    @Override
    protected IDeserializationConverter<?, ?> createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> {
                    if (val instanceof Byte) {
                        return new ByteColumn((Byte) val);
                    }

                    if (val instanceof Boolean) {
                        return new ByteColumn((byte) (((Boolean) val) ? 1 : 0));
                    }

                    if (val instanceof Integer) {
                        return new ByteColumn(Byte.parseByte(String.valueOf(val)));
                    }

                    throw new UnsupportedOperationException(
                            "Can not cast class [" + val.getClass().getName() + "] to Byte.");
                };
            case SMALLINT:
                return val -> {
                    if (val instanceof Integer) {
                        return new ShortColumn(Short.parseShort(String.valueOf(val)));
                    }

                    if (val instanceof Short) {
                        return new ShortColumn((Short) val);
                    }

                    throw new UnsupportedOperationException(
                            "Can not cast class [" + val.getClass().getName() + "] to SmallInt.");
                };
            case INTEGER:
                return val -> new IntColumn((Integer) val);
            case INTERVAL_YEAR_MONTH:
                return getYearMonthDeserialization((YearMonthIntervalType) type);
            case FLOAT:
                return val -> new FloatColumn((Float) val);
            case DOUBLE:
                return val -> new DoubleColumn((Double) val);
            case BIGINT:
                return val -> {
                    if (val instanceof Long) {
                        return new LongColumn((Long) val);
                    }

                    if (val instanceof BigInteger) {
                        return new LongColumn(((BigInteger) val).longValue());
                    }

                    throw new UnsupportedOperationException(
                            "Can not cast class [" + val.getClass().getName() + "] to Bigint.");
                };
            case DECIMAL:
                return val -> {
                    if (val instanceof BigInteger) {
                        return new BigDecimalColumn((BigInteger) val);
                    }
                    return new BigDecimalColumn((BigDecimal) val);
                };
            case CHAR:
            case VARCHAR:
                return val -> new StringColumn((String) val);
            case DATE:
                return val -> new SqlDateColumn((Date) val);
            case TIME_WITHOUT_TIME_ZONE:
                return val -> new TimeColumn((Time) val);
            case TIMESTAMP_WITH_TIME_ZONE:
                return getZonedTimestampDeserialization((ZonedTimestampType) type);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int precision = ((TimestampType) (type)).getPrecision();
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val -> new TimestampColumn((Timestamp) val, precision);

            case BINARY:
            case VARBINARY:
                return val -> {
                    if (val instanceof Boolean) {
                        return new BooleanColumn((Boolean) val);
                    }

                    return new BytesColumn((byte[]) val);
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
