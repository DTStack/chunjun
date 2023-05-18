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

package com.dtstack.chunjun.connector.sqlserver.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcSyncConverter;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
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
import com.dtstack.chunjun.element.column.ZonedTimestampColumn;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import microsoft.sql.DateTimeOffset;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import static com.dtstack.chunjun.connector.sqlserver.util.SqlUtil.timestampBytesToLong;

public class SqlserverMicroSoftSyncConverter extends JdbcSyncConverter {

    private static final long serialVersionUID = -8683172637123747253L;

    public SqlserverMicroSoftSyncConverter(RowType rowType, CommonConfig commonConfig) {
        super(rowType, commonConfig);
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(ResultSet resultSet) throws Exception {
        List<FieldConfig> fieldConfList = commonConfig.getColumn();
        ColumnRowData result = new ColumnRowData(fieldConfList.size());
        int converterIndex = 0;
        for (FieldConfig fieldConfig : fieldConfList) {
            AbstractBaseColumn baseColumn = null;
            if (StringUtils.isBlank(fieldConfig.getValue())) {
                Object field = resultSet.getObject(converterIndex + 1);
                // in sqlserver, timestamp type is a binary array of 8 bytes.
                baseColumn =
                        (AbstractBaseColumn)
                                toInternalConverters.get(converterIndex).deserialize(field);

                converterIndex++;
            }
            result.addField(assembleFieldProps(fieldConfig, baseColumn));
        }
        return result;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> new ByteColumn(((Short) val).byteValue());
            case SMALLINT:
                return val -> new ShortColumn(((Short) val));
            case INTEGER:
                return val -> new IntColumn((Integer) val);
            case FLOAT:
                return val -> new FloatColumn((Float) val);
            case DOUBLE:
                return val -> new DoubleColumn((Double) val);
            case BIGINT:
                if (type
                        instanceof
                        com.dtstack.chunjun.connector.sqlserver.converter.TimestampType) {
                    return val -> new LongColumn(timestampBytesToLong((byte[]) val));
                }
                return val -> new LongColumn((Long) val);
            case DECIMAL:
                return val -> new BigDecimalColumn((BigDecimal) val);
            case CHAR:
            case VARCHAR:
                return val -> new StringColumn((String) val);
            case DATE:
                return val -> new SqlDateColumn((Date) val);
            case TIME_WITHOUT_TIME_ZONE:
                return val -> new TimeColumn((Time) val);
            case TIMESTAMP_WITH_TIME_ZONE:
                int zonedPrecision = ((ZonedTimestampType) type).getPrecision();
                return val -> {
                    Timestamp timestamp = ((DateTimeOffset) val).getTimestamp();
                    int millisecondOffset = ((DateTimeOffset) val).getMinutesOffset() * 60 * 1000;
                    return new ZonedTimestampColumn(
                            timestamp.getTime(),
                            timestamp.getNanos(),
                            millisecondOffset,
                            zonedPrecision);
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val -> {
                            int precision = ((TimestampType) (type)).getPrecision();
                            return new TimestampColumn((Timestamp) val, precision);
                        };
            case BINARY:
            case VARBINARY:
                return val -> new BytesColumn((byte[]) val);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) ->
                        statement.setBoolean(
                                index, ((ColumnRowData) val).getField(index).asBoolean());
            case SMALLINT:
            case TINYINT:
                return (val, index, statement) -> statement.setShort(index, val.getShort(index));
            case INTEGER:
                return (val, index, statement) ->
                        statement.setInt(index, ((ColumnRowData) val).getField(index).asInt());
            case FLOAT:
                return (val, index, statement) ->
                        statement.setFloat(index, ((ColumnRowData) val).getField(index).asFloat());
            case DOUBLE:
                return (val, index, statement) ->
                        statement.setDouble(
                                index, ((ColumnRowData) val).getField(index).asDouble());

            case BIGINT:
                if (type
                        instanceof
                        com.dtstack.chunjun.connector.sqlserver.converter.TimestampType) {
                    return (val, index, statement) -> {
                        throw new UnsupportedTypeException(
                                "The timestampType of SQLServer can be used only for query");
                    };
                }
                return (val, index, statement) ->
                        statement.setLong(index, ((ColumnRowData) val).getField(index).asLong());
            case DECIMAL:
                return (val, index, statement) ->
                        statement.setBigDecimal(
                                index, ((ColumnRowData) val).getField(index).asBigDecimal());
            case CHAR:
            case VARCHAR:
                return (val, index, statement) ->
                        statement.setString(
                                index, ((ColumnRowData) val).getField(index).asString());
            case DATE:
                return (val, index, statement) ->
                        statement.setDate(index, ((ColumnRowData) val).getField(index).asSqlDate());
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTime(index, ((ColumnRowData) val).getField(index).asTime());
            case TIMESTAMP_WITH_TIME_ZONE:
                return (val, index, statement) -> {
                    AbstractBaseColumn field = ((ColumnRowData) val).getField(index);
                    if (field instanceof ZonedTimestampColumn) {
                        ZonedTimestampColumn zonedTimestampColumn = (ZonedTimestampColumn) field;
                        statement.setObject(
                                index,
                                DateTimeOffset.valueOf(
                                        zonedTimestampColumn.asTimestampWithUtc(),
                                        zonedTimestampColumn.getMillisecondOffset() / 1000 / 60));
                    } else {
                        statement.setObject(
                                index,
                                DateTimeOffset.valueOf(field.asTimestamp(), getMinutesOffset()));
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, ((ColumnRowData) val).getField(index).asTimestamp());

            case BINARY:
            case VARBINARY:
                return (val, index, statement) ->
                        statement.setBytes(index, ((ColumnRowData) val).getField(index).asBytes());
            default:
                throw new UnsupportedTypeException("Unsupported type:" + type);
        }
    }

    public int getMinutesOffset() {
        return getMillSecondOffset() / 1000 / 60;
    }

    public int getMillSecondDiffWithTimeZone(String sqlServerTimeZone) {
        long currentTimeMillis = System.currentTimeMillis();
        TimeZone timeZone = TimeZone.getTimeZone("GMT" + sqlServerTimeZone);
        return timeZone.getOffset(currentTimeMillis) - getMillSecondOffset(currentTimeMillis);
    }

    public int getMillSecondOffset(long time) {
        Calendar calendar = new GregorianCalendar();
        return calendar.getTimeZone().getOffset(time);
    }

    public int getMillSecondOffset() {
        Calendar calendar = new GregorianCalendar();
        return calendar.getTimeZone().getOffset(System.currentTimeMillis());
    }
}
