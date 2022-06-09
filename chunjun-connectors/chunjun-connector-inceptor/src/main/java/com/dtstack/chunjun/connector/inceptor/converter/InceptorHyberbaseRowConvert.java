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

package com.dtstack.chunjun.connector.inceptor.converter;

import com.dtstack.chunjun.connector.jdbc.converter.JdbcRowConverter;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.ColumnRowData;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.hive.common.type.HiveDate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Calendar;

import static com.dtstack.chunjun.connector.inceptor.util.InceptorDbUtil.LOCAL_TIMEZONE;

/** @author liuliu 2022/2/25 */
public class InceptorHyberbaseRowConvert extends JdbcRowConverter {

    public InceptorHyberbaseRowConvert(RowType rowType) {
        super(rowType);
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case INTEGER:
            case BOOLEAN:
            case BIGINT:
            case DOUBLE:
                return val -> val;
            case SMALLINT:
                return val -> ((Integer) val).shortValue();
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return val -> DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val -> {
                    HiveDate hiveDate = (HiveDate) val;
                    long time = hiveDate.getTime();
                    Calendar c = Calendar.getInstance(LOCAL_TIMEZONE.get());
                    c.setTime(hiveDate);
                    if (c.get(Calendar.HOUR_OF_DAY) == 0
                            && c.get(Calendar.MINUTE) == 0
                            && c.get(Calendar.SECOND) == 0) {
                        return hiveDate.toLocalDate().toEpochDay();
                    } else {
                        return TimestampData.fromEpochMillis(time);
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromEpochMillis(((Timestamp) val).getTime());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case VARCHAR:
                return (val, index, statement) ->
                        statement.setString(
                                index, ((ColumnRowData) val).getField(index).asString());
            case INTEGER:
            case SMALLINT:
                return (val, index, statement) ->
                        statement.setInt(index, ((ColumnRowData) val).getField(index).asInt());
            case BOOLEAN:
                return (val, index, statement) ->
                        statement.setBoolean(
                                index, ((ColumnRowData) val).getField(index).asBoolean());
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index, val.getByte(index));
            case BIGINT:
                return (val, index, statement) ->
                        statement.setLong(index, ((ColumnRowData) val).getField(index).asLong());
            case DOUBLE:
                return (val, index, statement) ->
                        statement.setDouble(
                                index, ((ColumnRowData) val).getField(index).asDouble());
            case DATE:
                return (val, index, statement) ->
                        statement.setDate(
                                index,
                                new HiveDate(
                                        ((ColumnRowData) val)
                                                .getField(index)
                                                .asTimestamp()
                                                .getTime()));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, ((ColumnRowData) val).getField(index).asTimestamp());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
