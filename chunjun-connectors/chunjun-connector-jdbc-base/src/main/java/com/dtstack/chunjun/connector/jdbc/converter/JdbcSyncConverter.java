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

package com.dtstack.chunjun.connector.jdbc.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
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
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.element.column.YearMonthColumn;
import com.dtstack.chunjun.element.column.ZonedTimestampColumn;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import io.vertx.core.json.JsonArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

/** Base class for all converters that convert between JDBC object and Flink internal object. */
@Slf4j
public class JdbcSyncConverter
        extends AbstractRowConverter<
                ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType> {

    private static final long serialVersionUID = -6344403538183946505L;

    public JdbcSyncConverter(RowType rowType) {
        this(rowType, null);
    }

    public JdbcSyncConverter(RowType rowType, CommonConfig commonConfig) {
        super(rowType, commonConfig);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement>
            wrapIntoNullableExternalConverter(
                    ISerializationConverter<FieldNamedPreparedStatement> serializationConverter,
                    LogicalType type) {
        return (val, index, statement) -> {
            if (((ColumnRowData) val).getField(index) == null
                    || ((ColumnRowData) val).getField(index) instanceof NullColumn) {
                statement.setObject(index, null);
            } else {
                serializationConverter.serialize(val, index, statement);
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(ResultSet resultSet) throws Exception {
        List<FieldConfig> fieldConfigList = commonConfig.getColumn();
        ColumnRowData result;
        if (fieldConfigList.size() == 1
                && ConstantValue.STAR_SYMBOL.equals(fieldConfigList.get(0).getName())) {
            result = new ColumnRowData(fieldTypes.length);
            for (int index = 0; index < fieldTypes.length; index++) {
                Object field = resultSet.getObject(index + 1);
                AbstractBaseColumn baseColumn =
                        (AbstractBaseColumn) toInternalConverters.get(index).deserialize(field);
                result.addField(baseColumn);
            }
            return result;
        }
        int converterIndex = 0;
        result = new ColumnRowData(fieldConfigList.size());
        for (FieldConfig fieldConfig : fieldConfigList) {
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
    public FieldNamedPreparedStatement toExternal(
            RowData rowData, FieldNamedPreparedStatement statement) throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, statement);
        }
        return statement;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> new ByteColumn((Byte) val);
            case SMALLINT:
                return val -> new ShortColumn((Short) val);
            case INTEGER:
                return val -> new IntColumn((Integer) val);
            case INTERVAL_YEAR_MONTH:
                return getYearMonthDeserialization((YearMonthIntervalType) type);
            case FLOAT:
                return val -> new FloatColumn((Float) val);
            case DOUBLE:
                return val -> new DoubleColumn((Double) val);
            case BIGINT:
                return val -> new LongColumn((Long) val);
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
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index, val.getByte(index));
            case SMALLINT:
            case INTEGER:
                return (val, index, statement) ->
                        statement.setInt(index, ((ColumnRowData) val).getField(index).asInt());
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
                return getZonedTimestampSerialization();
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

    protected IDeserializationConverter<Object, ZonedTimestampColumn>
            getZonedTimestampDeserialization(ZonedTimestampType type) {
        int precision = type.getPrecision();
        return val -> new ZonedTimestampColumn((Timestamp) val, precision);
    }

    protected ISerializationConverter<FieldNamedPreparedStatement>
            getZonedTimestampSerialization() {
        return (val, index, statement) ->
                statement.setTimestamp(index, ((ColumnRowData) val).getField(index).asTimestamp());
    }

    public IDeserializationConverter<?, ?> getYearMonthDeserialization(
            YearMonthIntervalType yearMonthIntervalType) {
        switch (yearMonthIntervalType.getResolution()) {
            case YEAR:
            case MONTH:
            case YEAR_TO_MONTH:
                return val -> {
                    if (val instanceof Date) {
                        Date date = (Date) val;
                        return new YearMonthColumn(
                                date.toLocalDate().getYear() * 12
                                        + date.toLocalDate().getMonthValue());
                    }
                    return new YearMonthColumn(Integer.parseInt(String.valueOf(val)));
                };
            default:
                throw new UnsupportedOperationException(
                        yearMonthIntervalType.getResolution().name());
        }
    }

    protected ISerializationConverter<FieldNamedPreparedStatement> getYearMonthSerialization(
            YearMonthIntervalType yearMonthIntervalType) {
        switch (yearMonthIntervalType.getResolution()) {
            case YEAR:
                return (val, index, statement) ->
                        statement.setInt(index, ((ColumnRowData) val).getField(index).asYearInt());
            case MONTH:
                return (val, index, statement) ->
                        statement.setInt(index, ((ColumnRowData) val).getField(index).asMonthInt());
            case YEAR_TO_MONTH:
                return (val, index, statement) ->
                        statement.setString(
                                index, ((ColumnRowData) val).getField(index).asString());
            default:
                throw new UnsupportedOperationException(
                        yearMonthIntervalType.getResolution().name());
        }
    }
}
