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

package com.dtstack.flinkx.connector.inceptor.converter;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.SqlDateColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.hive.common.type.HiveDate;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;

import static com.dtstack.flinkx.connector.inceptor.util.InceptorDbUtil.LOCAL_TIMEZONE;

/** @author liuliu 2022/2/25 */
public class InceptorHyberbaseColumnConvert extends JdbcColumnConverter {

    public InceptorHyberbaseColumnConvert(RowType rowType, FlinkxCommonConf commonConf) {
        super(rowType, commonConf);
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement>
            wrapIntoNullableExternalConverter(
                    ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, statement) -> {
            if (((ColumnRowData) val).getField(index) == null) {
                statement.setNull(index, 0);
            } else {
                serializationConverter.serialize(val, index, statement);
            }
        };
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case VARCHAR:
                return val -> new StringColumn((String) val);
            case INTEGER:
            case SMALLINT:
                return val -> new BigDecimalColumn(new BigDecimal(val.toString()).intValue());
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> new BigDecimalColumn(new BigDecimal(val.toString()).byteValue());
            case BIGINT:
                return val -> new BigDecimalColumn((Long) val);
            case DOUBLE:
                return val -> new BigDecimalColumn((Double) val);
            case DECIMAL:
                return val -> new BigDecimalColumn((BigDecimal) val);
            case DATE:
                return val -> {
                    HiveDate hiveDate = (HiveDate) val;
                    long time = hiveDate.getTime();
                    Calendar c = Calendar.getInstance(LOCAL_TIMEZONE.get());
                    c.setTime(hiveDate);
                    if (c.get(Calendar.HOUR_OF_DAY) == 0
                            && c.get(Calendar.MINUTE) == 0
                            && c.get(Calendar.SECOND) == 0) {
                        return new SqlDateColumn(new Date(time));
                    } else {
                        return new TimestampColumn(time, 0);
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> new TimestampColumn((Timestamp) val, 9);
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
            case DECIMAL:
                return (val, index, statement) ->
                        statement.setBigDecimal(
                                index, ((ColumnRowData) val).getField(index).asBigDecimal());
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
