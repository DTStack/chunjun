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

package com.dtstack.chunjun.connector.sybase.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcSyncConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class SybaseSyncConverter extends JdbcSyncConverter {
    private static final long serialVersionUID = 5539444089187873362L;

    public SybaseSyncConverter(RowType rowType, CommonConfig commonConfig) {
        super(rowType, commonConfig);
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> {
                    // compatible with BIT(>1)
                    if (val instanceof byte[]) {
                        return new BytesColumn((byte[]) val);
                    } else {
                        return new BooleanColumn(Boolean.parseBoolean(val.toString()));
                    }
                };
            case TINYINT:
                return val -> {
                    if (val instanceof Boolean) {
                        return new BigDecimalColumn(((boolean) val) ? 1 : 0);
                    } else {
                        return new BigDecimalColumn((Integer) val);
                    }
                };
            case SMALLINT:
            case INTEGER:
                return val -> new BigDecimalColumn((Integer) val);
            case INTERVAL_YEAR_MONTH:
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val -> {
                            YearMonthIntervalType yearMonthIntervalType =
                                    (YearMonthIntervalType) type;
                            switch (yearMonthIntervalType.getResolution()) {
                                case YEAR:
                                    return new BigDecimalColumn(
                                            Integer.parseInt(String.valueOf(val).substring(0, 4)));
                                case MONTH:
                                case YEAR_TO_MONTH:
                                default:
                                    throw new UnsupportedOperationException(
                                            "jdbc converter only support YEAR");
                            }
                        };
            case FLOAT:
                return val -> {
                    if (val instanceof Double) {
                        BigDecimal b = new BigDecimal(String.valueOf(val));
                        return new DoubleColumn(b.doubleValue());
                    }
                    return new FloatColumn((Float) val);
                };
            case DOUBLE:
                return val -> new DoubleColumn((Double) val);
            case BIGINT:
                return val -> {
                    if (val instanceof Integer) {
                        return new BigDecimalColumn((Integer) val);
                    }
                    return new BigDecimalColumn((Long) val);
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
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val ->
                                new TimestampColumn(
                                        (Timestamp) val, ((TimestampType) (type)).getPrecision());

            case BINARY:
            case VARBINARY:
                return val -> new BytesColumn((byte[]) val);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
