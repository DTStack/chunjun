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

package com.dtstack.chunjun.connector.dm.converter;

import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.StringUtil;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;

import dm.jdbc.driver.DmdbBlob;
import dm.jdbc.driver.DmdbClob;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/** @author kunni */
public class DmColumnConverter extends JdbcColumnConverter {

    public DmColumnConverter(RowType rowType, ChunJunCommonConf commonConf) {
        super(rowType, commonConf);
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> {
                    if (val instanceof Integer) {
                        return new BigDecimalColumn(((Integer) val).byteValue());
                    } else if (val instanceof Short) {
                        return new BigDecimalColumn(((Short) val).byteValue());
                    } else {
                        return new BigDecimalColumn((Byte) val);
                    }
                };
            case SMALLINT:
            case INTEGER:
                return val -> {
                    if (val instanceof Byte) {
                        return new BigDecimalColumn(((Byte) val).intValue());
                    } else if (val instanceof Short) {
                        return new BigDecimalColumn(((Short) val).intValue());
                    } else {
                        return new BigDecimalColumn((Integer) val);
                    }
                };
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
                return val -> new BigDecimalColumn((Float) val);
            case DOUBLE:
                return val -> new BigDecimalColumn((Double) val);
            case BIGINT:
                return val -> new BigDecimalColumn((Long) val);
            case DECIMAL:
                return val -> new BigDecimalColumn((BigDecimal) val);
            case CHAR:
            case VARCHAR:
                return val -> {
                    // support text type
                    if (val instanceof DmdbClob) {
                        try {
                            return new StringColumn(
                                    StringUtil.inputStream2String(
                                            ((DmdbClob) val).getAsciiStream()));
                        } catch (Exception e) {
                            throw new UnsupportedOperationException(
                                    "failed to get length from text");
                        }
                    } else if (val instanceof DmdbBlob) {
                        try {
                            return new StringColumn(
                                    StringUtil.inputStream2String(
                                            ((DmdbBlob) val).getBinaryStream()));
                        } catch (Exception e) {
                            throw new UnsupportedOperationException(
                                    "failed to get length from text");
                        }
                    } else {
                        return new StringColumn((String) val);
                    }
                };
            case DATE:
                return val -> new SqlDateColumn(Date.valueOf(String.valueOf(val)));
            case TIME_WITHOUT_TIME_ZONE:
                return val -> new TimeColumn(Time.valueOf(String.valueOf(val)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> new TimestampColumn((Timestamp) val);
            case BINARY:
            case VARBINARY:
                return val -> {
                    if (val instanceof DmdbBlob) {
                        try {
                            return new StringColumn(
                                    StringUtil.inputStream2String(
                                            ((DmdbBlob) val).getBinaryStream()));
                        } catch (Exception e) {
                            throw new UnsupportedOperationException(
                                    "failed to get length from blob");
                        }
                    }
                    return new BytesColumn((byte[]) val);
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
