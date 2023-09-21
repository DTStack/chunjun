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

package com.dtstack.chunjun.connector.oracle.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcSyncConverter;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.constants.ConstantValue;
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
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;

import oracle.sql.TIMESTAMP;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

public class OracleSyncConverter extends JdbcSyncConverter {

    private static final long serialVersionUID = 4264798795927398020L;

    private static final Calendar var4 =
            Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.US);
    private int currentIndex = 0;

    public OracleSyncConverter(RowType rowType, CommonConfig commonConfig) {
        super(rowType, commonConfig);
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(ResultSet resultSet) throws Exception {
        List<FieldConfig> fieldConfList = commonConfig.getColumn();
        ColumnRowData result;
        if (fieldConfList.size() == 1
                && ConstantValue.STAR_SYMBOL.equals(fieldConfList.get(0).getName())) {
            result = new ColumnRowData(fieldTypes.length);
            for (int index = 0; index < fieldTypes.length; index++) {
                Object field = resultSet.getObject(index + 1);
                AbstractBaseColumn baseColumn =
                        (AbstractBaseColumn) toInternalConverters.get(index).deserialize(field);
                result.addField(baseColumn);
            }
            return result;
        }
        currentIndex = 0;
        result = new ColumnRowData(fieldConfList.size());
        for (FieldConfig fieldConfig : fieldConfList) {
            AbstractBaseColumn baseColumn = null;
            if (StringUtils.isBlank(fieldConfig.getValue())) {
                baseColumn =
                        (AbstractBaseColumn)
                                toInternalConverters.get(currentIndex).deserialize(resultSet);
                currentIndex++;
            }
            result.addField(assembleFieldProps(fieldConfig, baseColumn));
        }
        return result;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            Object object = field.getObject(currentIndex + 1);
                            if (object == null) {
                                return null;
                            }
                            return new BooleanColumn(Boolean.parseBoolean(object.toString()));
                        };
            case TINYINT:
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            Object object = field.getObject(currentIndex + 1);
                            if (object == null) {
                                return null;
                            }
                            return new ByteColumn(((Integer) object).byteValue());
                        };
            case SMALLINT:
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            String object = field.getString(currentIndex + 1);
                            if (object == null) {
                                return null;
                            }
                            return new ShortColumn(Short.parseShort(object));
                        };
            case INTEGER:
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            String object = field.getString(currentIndex + 1);
                            if (object == null) {
                                return null;
                            }
                            return new IntColumn(Integer.parseInt(object));
                        };
            case FLOAT:
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            String object = field.getString(currentIndex + 1);
                            if (object == null) {
                                return null;
                            }
                            return new FloatColumn(Float.parseFloat(object));
                        };
            case DOUBLE:
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            String object = field.getString(currentIndex + 1);
                            if (object == null) {
                                return null;
                            }
                            return new DoubleColumn(Double.parseDouble(object));
                        };
            case BIGINT:
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            String object = field.getString(currentIndex + 1);
                            if (object == null) {
                                return null;
                            }
                            return new LongColumn(Long.parseLong(object));
                        };
            case DECIMAL:
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            String object = field.getString(currentIndex + 1);
                            if (object == null) {
                                return null;
                            }
                            return new BigDecimalColumn(object);
                        };

            case CHAR:
            case VARCHAR:
                if (type instanceof ClobType) {
                    return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                            field -> {
                                Object object = field.getObject(currentIndex + 1);
                                if (object == null) {
                                    return null;
                                }
                                oracle.sql.CLOB clob = (oracle.sql.CLOB) object;
                                return new StringColumn(ConvertUtil.convertClob(clob));
                            };
                }
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            String object = field.getString(currentIndex + 1);
                            if (object == null) {
                                return null;
                            }
                            return new StringColumn(object);
                        };
            case DATE:
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            Timestamp object = field.getTimestamp(currentIndex + 1);
                            if (object == null) {
                                return null;
                            }
                            return new TimestampColumn(object, 0);
                        };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            int precision = ((TimestampType) (type)).getPrecision();
                            Timestamp object = field.getTimestamp(currentIndex + 1);
                            if (object == null) {
                                return null;
                            }
                            return new TimestampColumn(object, precision);
                        };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                        field -> {
                            Object val = field.getObject(currentIndex + 1);
                            if (val == null) {
                                return null;
                            }
                            if (val instanceof TIMESTAMP) {
                                return new TimestampColumn(((TIMESTAMP) val).timestampValue());
                            } else if (val instanceof oracle.sql.TIMESTAMPLTZ) {
                                return new TimestampColumn(
                                        ((oracle.sql.TIMESTAMPLTZ) val)
                                                .timestampValue(null, Calendar.getInstance()));
                            } else if (val instanceof oracle.sql.TIMESTAMPTZ) {
                                return new TimestampColumn(
                                        (toTimestamp2(
                                                ((oracle.sql.TIMESTAMPTZ) val).getBytes(), null)));
                            } else if (val instanceof Timestamp) {
                                return new TimestampColumn((Timestamp) val);
                            }
                            throw new RuntimeException("not support type" + val.getClass());
                        };
            case BINARY:
            case VARBINARY:
                if (type instanceof BlobType) {
                    return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                            field -> {
                                Object object = field.getObject(currentIndex + 1);
                                if (object == null) {
                                    return null;
                                }
                                oracle.sql.BLOB blob = (oracle.sql.BLOB) object;
                                byte[] bytes = ConvertUtil.toByteArray(blob);
                                return new BytesColumn(bytes);
                            };
                } else {
                    return (IDeserializationConverter<ResultSet, AbstractBaseColumn>)
                            field -> {
                                byte[] object = field.getBytes(currentIndex + 1);
                                if (object == null) {
                                    return null;
                                }
                                return new BytesColumn(object);
                            };
                }
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement>
            wrapIntoNullableExternalConverter(
                    ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, statement) -> {
            if (((ColumnRowData) val).getField(index) == null
                    || ((ColumnRowData) val).getField(index) instanceof NullColumn) {
                try {
                    final int sqlType =
                            JdbcTypeUtil.typeInformationToSqlType(
                                    TypeConversions.fromDataTypeToLegacyInfo(
                                            TypeConversions.fromLogicalToDataType(type)));
                    statement.setNull(index, sqlType);
                } catch (Exception e) {
                    statement.setObject(index, null);
                }
            } else {
                serializationConverter.serialize(val, index, statement);
            }
        };
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
                return (val, index, statement) -> {
                    if (type instanceof ClobType) {
                        try (StringReader reader =
                                new StringReader(
                                        ((ColumnRowData) val).getField(index).asString())) {
                            statement.setClob(index, reader);
                        }
                    } else {
                        statement.setString(
                                index, ((ColumnRowData) val).getField(index).asString());
                    }
                };
            case DATE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, ((ColumnRowData) val).getField(index).asTimestamp());

            case BINARY:
            case VARBINARY:
                return (val, index, statement) -> {
                    if (type instanceof BlobType) {
                        try (InputStream is = new ByteArrayInputStream(val.getBinary(index))) {
                            statement.setBlob(index, is);
                        }
                    } else {
                        statement.setBytes(index, val.getBinary(index));
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public static Timestamp toTimestamp2(byte[] var1, String zone) {
        int[] var2 = new int[13];

        int var3;
        for (var3 = 0; var3 < 13; ++var3) {
            var2[var3] = var1[var3] & 255;
        }

        var3 = TIMESTAMP.getJavaYear(var2[0], var2[1]);

        var4.clear();
        var4.set(1, var3);
        var4.set(2, var2[2] - 1);
        var4.set(5, var2[3]);
        var4.set(11, var2[4] - 1);
        var4.set(12, var2[5] - 1);
        var4.set(13, var2[6] - 1);
        var4.set(14, 0);
        long var5 = var4.getTime().getTime();
        Timestamp var7 = new Timestamp(var5);
        int var8 = TIMESTAMP.getNanos(var1, 7);
        var7.setNanos(var8);
        if (StringUtils.isNotBlank(zone)) {
            var7 = Timestamp.valueOf(var7.toInstant().atZone(ZoneId.of(zone)).toLocalDateTime());
        }
        return var7;
    }
}
