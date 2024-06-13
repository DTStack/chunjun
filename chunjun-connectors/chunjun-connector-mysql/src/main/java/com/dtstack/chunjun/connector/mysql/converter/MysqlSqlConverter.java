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

package com.dtstack.chunjun.connector.mysql.converter;

import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.util.ExternalDataUtil;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.utils.TypeConversions;

import io.vertx.core.json.JsonArray;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** mysql sql converter */
@Slf4j
public class MysqlSqlConverter
        extends AbstractRowConverter<
                ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType> {

    private static final long serialVersionUID = -931022595406994406L;

    public MysqlSqlConverter(RowType rowType) {
        super(rowType);
        List<RowType.RowField> fields = rowType.getFields();
        for (RowType.RowField field : fields) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(createInternalConverter(field)));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(field), field.getType()));
        }
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement>
            wrapIntoNullableExternalConverter(
                    ISerializationConverter<FieldNamedPreparedStatement> serializationConverter,
                    LogicalType type) {
        int sqlType = 0;
        try {
            // Exclude nested data types, such as ROW(id int,data ROW(id string))
            sqlType =
                    JdbcTypeUtil.typeInformationToSqlType(
                            TypeConversions.fromDataTypeToLegacyInfo(
                                    TypeConversions.fromLogicalToDataType(type)));
        } catch (IllegalArgumentException e) {
            log.warn(e.getMessage());
        }
        int finalSqlType = sqlType;
        return (val, index, statement) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                statement.setNull(index, finalSqlType);
            } else {
                serializationConverter.serialize(val, index, statement);
            }
        };
    }

    @Override
    public RowData toInternal(ResultSet resultSet) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = resultSet.getObject(pos + 1);
            if (resultSet.getMetaData().getColumnTypeName(pos + 1).equals("BIGINT UNSIGNED")
                    && field != null) {
                field = ((BigInteger) field).longValue();
            }
            if (resultSet.getMetaData().getColumnTypeName(pos + 1).equals("INT UNSIGNED")
                    && field != null) {
                field = ((Long) field).intValue();
            }
            genericRowData.setField(pos, toInternalConverters.get(pos).deserialize(field));
        }
        return genericRowData;
    }

    @Override
    public RowData toInternalLookup(JsonArray jsonArray) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = jsonArray.getValue(pos);
            // 当sql里声明的字段类型为BIGINT时，将BigInteger (BIGINT UNSIGNED) 转换为Long
            if (rowType.getFields()
                            .get(pos)
                            .getType()
                            .getTypeRoot()
                            .name()
                            .equalsIgnoreCase("BIGINT")
                    && field instanceof BigInteger) {
                field = ((BigInteger) field).longValue();
            }
            // 当sql里声明的字段类型为INT时，将Long (INT UNSIGNED) 转换为Integer
            if (rowType.getFields()
                            .get(pos)
                            .getType()
                            .getTypeRoot()
                            .name()
                            .equalsIgnoreCase("INTEGER")
                    && field instanceof Long) {
                field = ((Long) field).intValue();
            }
            genericRowData.setField(pos, toInternalConverters.get(pos).deserialize(field));
        }
        return genericRowData;
    }

    @Override
    public FieldNamedPreparedStatement toExternal(
            RowData rowData, FieldNamedPreparedStatement statement) throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, statement);
        }
        return statement;
    }

    protected IDeserializationConverter createInternalConverter(RowType.RowField rowField) {
        LogicalType type = rowField.getType();
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
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val ->
                        val instanceof Timestamp
                                ? (int)
                                        (((Timestamp) val)
                                                .toLocalDateTime()
                                                .toLocalDate()
                                                .toEpochDay())
                                : (int)
                                        ((Date.valueOf(String.valueOf(val)))
                                                .toLocalDate()
                                                .toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int)
                                ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    if (val instanceof LocalDateTime) {
                        return TimestampData.fromTimestamp(Timestamp.valueOf((LocalDateTime) val));
                    }
                    return TimestampData.fromTimestamp((Timestamp) val);
                };
            case CHAR:
            case VARCHAR:
                return val -> val == null ? null : StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> (byte[]) val;
            case ARRAY:
                return (val) -> {
                    Array val1 = (Array) val;
                    Object[] array = (Object[]) val1.getArray();
                    Object[] result = new Object[array.length];
                    LogicalType logicalType = type.getChildren().get(0);
                    RowType.RowField rowField1 = new RowType.RowField("", logicalType, "");
                    IDeserializationConverter internalConverter =
                            createInternalConverter(rowField1);
                    for (int i = 0; i < array.length; i++) {
                        Object value = internalConverter.deserialize(array[i]);
                        result[i] = value;
                    }
                    return new GenericArrayData(result);
                };

            case ROW:
                return val -> {
                    List<RowType.RowField> childrenFields = ((RowType) type).getFields();
                    HashMap childrenData = GsonUtil.GSON.fromJson(val.toString(), HashMap.class);
                    GenericRowData genericRowData = new GenericRowData(childrenFields.size());
                    for (int i = 0; i < childrenFields.size(); i++) {
                        Object value =
                                createInternalConverter(childrenFields.get(i))
                                        .deserialize(
                                                childrenData.get(childrenFields.get(i).getName()));
                        genericRowData.setField(i, value);
                    }
                    return genericRowData;
                };
            case MAP:
                return val -> {
                    if (val == null) {
                        return null;
                    }
                    HashMap<Object, Object> resultMap = new HashMap<>();
                    Map map = GsonUtil.GSON.fromJson(val.toString(), Map.class);
                    LogicalType keyType = ((MapType) type).getKeyType();
                    LogicalType valueType = ((MapType) type).getValueType();
                    RowType.RowField keyRowField = new RowType.RowField("", keyType, "");
                    RowType.RowField valueRowField = new RowType.RowField("", valueType, "");
                    IDeserializationConverter keyInternalConverter =
                            createInternalConverter(keyRowField);
                    IDeserializationConverter valueInternalConverter =
                            createInternalConverter(valueRowField);
                    for (Object key : map.keySet()) {
                        resultMap.put(
                                keyInternalConverter.deserialize(key),
                                valueInternalConverter.deserialize(map.get(key)));
                    }

                    return new GenericMapData(resultMap);
                };
            case STRUCTURED_TYPE:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    protected ISerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
            RowType.RowField rowField) {
        LogicalType type = rowField.getType();
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) ->
                        statement.setBoolean(index, val.getBoolean(index));
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index, val.getByte(index));
            case SMALLINT:
                return (val, index, statement) -> statement.setShort(index, val.getShort(index));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, index, statement) -> statement.setInt(index, val.getInt(index));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, index, statement) -> statement.setLong(index, val.getLong(index));
            case FLOAT:
                return (val, index, statement) -> statement.setFloat(index, val.getFloat(index));
            case DOUBLE:
                return (val, index, statement) -> statement.setDouble(index, val.getDouble(index));
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (val, index, statement) ->
                        statement.setString(index, val.getString(index).toString());
            case BINARY:
            case VARBINARY:
                return (val, index, statement) -> statement.setBytes(index, val.getBinary(index));
            case DATE:
                return (val, index, statement) ->
                        statement.setDate(
                                index, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTime(
                                index,
                                Time.valueOf(
                                        LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, val.getTimestamp(index, timestampPrecision).toTimestamp());
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index, statement) ->
                        statement.setBigDecimal(
                                index,
                                val.getDecimal(index, decimalPrecision, decimalScale)
                                        .toBigDecimal());
            case ROW:
                return (val, index, statement) -> {
                    List<RowType.RowField> fields = ((RowType) type).getFields();
                    HashMap<String, Object> map = new HashMap<>();
                    for (int i = 0; i < fields.size(); i++) {
                        ExternalDataUtil.rowDataToExternal(
                                val.getRow(index, fields.size()),
                                i,
                                fields.get(i).getType(),
                                map,
                                fields.get(i).getName());
                    }

                    statement.setObject(index, GsonUtil.GSON.toJson(map));
                };

            case ARRAY:
                return (val, index, statement) -> {
                    Connection connection = statement.getConnection();
                    ArrayData array = val.getArray(index);
                    Object[] obj = new Object[array.size()];
                    ExternalDataUtil.arrayDataToExternal(type.getChildren().get(0), obj, array);
                    Array result =
                            connection.createArrayOf(
                                    type.getChildren().get(0).getTypeRoot().name(), obj);
                    statement.setArray(index, result);
                };
            case MULTISET:
                return (val, index, statement) -> {
                    Connection connection = statement.getConnection();
                    MapData map = val.getMap(index);
                    ArrayData arrayData = map.keyArray();
                    Object[] obj = new Object[arrayData.size()];
                    ExternalDataUtil.arrayDataToExternal(type.getChildren().get(0), obj, arrayData);
                    Array result =
                            connection.createArrayOf(
                                    type.getChildren().get(0).getTypeRoot().name(), obj);
                    statement.setArray(index, result);
                };
            case MAP:
                return (val, index, statement) -> {
                    MapData map = val.getMap(index);
                    Map<Object, Object> resultMap = new HashMap<>();
                    ExternalDataUtil.mapDataToExternal(
                            map,
                            ((MapType) type).getKeyType(),
                            ((MapType) type).getValueType(),
                            resultMap);
                    statement.setObject(index, resultMap);
                };
            case STRUCTURED_TYPE:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
