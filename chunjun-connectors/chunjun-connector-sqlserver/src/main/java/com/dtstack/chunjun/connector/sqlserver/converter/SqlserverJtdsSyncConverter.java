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
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import net.sourceforge.jtds.jdbc.BlobImpl;
import net.sourceforge.jtds.jdbc.ClobImpl;
import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import static com.dtstack.chunjun.connector.sqlserver.util.SqlUtil.timestampBytesToLong;

public class SqlserverJtdsSyncConverter extends JdbcSyncConverter {

    private static final long serialVersionUID = -8004349997103749874L;

    public SqlserverJtdsSyncConverter(RowType rowType, CommonConfig commonConfig) {
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
                return val -> new ByteColumn(Byte.parseByte(val.toString()));
            case SMALLINT:
                return val -> new ShortColumn(((Integer) val).shortValue());
            case INTEGER:
                return val -> new IntColumn((Integer) val);
            case FLOAT:
                return val -> new FloatColumn(Float.parseFloat(val.toString()));
            case DOUBLE:
                return val -> new DoubleColumn(Double.parseDouble(val.toString()));
            case BIGINT:
                return val -> new LongColumn(Long.parseLong(val.toString()));
            case DECIMAL:
                if (type
                        instanceof
                        com.dtstack.chunjun.connector.sqlserver.converter.TimestampType) {
                    return val -> new BigDecimalColumn(timestampBytesToLong((byte[]) val));
                }
                return val -> new BigDecimalColumn(val.toString());
            case CHAR:
            case VARCHAR:
                return val -> {
                    if (val instanceof ClobImpl) {
                        ClobImpl clob = (ClobImpl) val;
                        return new StringColumn(clob.getSubString(1, (int) clob.length()));
                    } else {
                        return new StringColumn(val.toString());
                    }
                };
            case DATE:
                return val -> new SqlDateColumn(Date.valueOf(val.toString()));
            case TIME_WITHOUT_TIME_ZONE:
                return val -> {
                    String[] split = val.toString().split("\\.");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");
                    long time = simpleDateFormat.parse(split[0]).getTime();
                    if (split.length > 1) {
                        time += Long.parseLong(split[1]);
                    }
                    return new TimeColumn(new Time(time));
                };
            case TIMESTAMP_WITH_TIME_ZONE:
                int zonedPrecision = ((ZonedTimestampType) type).getPrecision();
                return val -> {
                    String[] timeAndTimeZone = String.valueOf(val).split(" ");
                    if (timeAndTimeZone.length == 3) {
                        Timestamp timestamp =
                                Timestamp.valueOf(timeAndTimeZone[0] + " " + timeAndTimeZone[1]);
                        return new ZonedTimestampColumn(
                                timestamp, getTimeZone(timeAndTimeZone[2]), zonedPrecision);
                    } else {
                        return new TimestampColumn(
                                Timestamp.valueOf(timeAndTimeZone[0]), zonedPrecision);
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int precision = ((TimestampType) (type)).getPrecision();
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val -> new TimestampColumn(Timestamp.valueOf(val.toString()), precision);
            case BINARY:
            case VARBINARY:
                return val -> {
                    if (val instanceof BlobImpl) {
                        BlobImpl blob = (BlobImpl) val;
                        byte[] bytes = new byte[(int) blob.length()];
                        blob.getBinaryStream().read(bytes);
                        return new BytesColumn(bytes);
                    } else if (val instanceof byte[]) {
                        return new BytesColumn((byte[]) val);
                    } else {
                        throw new ChunJunRuntimeException("Unsupported type: " + val.getClass());
                    }
                };
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
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index, val.getByte(index));
            case SMALLINT:
                return (val, index, statement) -> statement.setShort(index, val.getShort(index));
            case INTEGER:
                return (val, index, statement) -> statement.setInt(index, val.getInt(index));
            case INTERVAL_YEAR_MONTH:
                return getYearMonthSerialization((YearMonthIntervalType) type);
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
            case TIMESTAMP_WITH_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setString(
                                index, ((ColumnRowData) val).getField(index).asString());
            case DATE:
                return (val, index, statement) ->
                        statement.setDate(index, ((ColumnRowData) val).getField(index).asSqlDate());
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTime(index, ((ColumnRowData) val).getField(index).asTime());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, ((ColumnRowData) val).getField(index).asTimestamp());

            case BINARY:
            case VARBINARY:
                return (val, index, statement) ->
                        statement.setBytes(index, ((ColumnRowData) val).getField(index).asBytes());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public int getMinutesOffset() {
        return getMillSecondOffset() / 1000 / 60;
    }

    public TimeZone getTimeZone(String sqlServerTimeZone) {
        return TimeZone.getTimeZone("GMT" + sqlServerTimeZone);
    }

    public int getMillSecondOffset() {
        Calendar calendar = new GregorianCalendar();
        return calendar.getTimeZone().getOffset(System.currentTimeMillis());
    }
}
