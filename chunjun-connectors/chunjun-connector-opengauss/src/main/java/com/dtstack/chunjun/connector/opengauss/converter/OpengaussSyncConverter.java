/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.opengauss.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcSyncConverter;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.opengauss.converter.logical.BitType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.JsonType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.JsonbType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.MoneyType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.PgCustomType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.PointType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.UuidType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.VarbitType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.XmlType;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.element.column.YearMonthColumn;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;

import org.opengauss.core.BaseConnection;
import org.opengauss.jdbc.PgArray;
import org.opengauss.jdbc.PgSQLXML;
import org.opengauss.util.PGobject;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;

public class OpengaussSyncConverter extends JdbcSyncConverter {

    private static final long serialVersionUID = 7381732546960782333L;

    private transient BaseConnection connection;

    public OpengaussSyncConverter(RowType rowType, CommonConfig commonConfig) {
        super(rowType, commonConfig);
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn((Boolean) val);
            case INTEGER:
                return val -> new IntColumn((Integer) val);
            case INTERVAL_YEAR_MONTH:
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val -> {
                            YearMonthIntervalType yearMonthIntervalType =
                                    (YearMonthIntervalType) type;
                            switch (yearMonthIntervalType.getResolution()) {
                                case YEAR:
                                    return new YearMonthColumn(
                                            Integer.parseInt(String.valueOf(val).substring(0, 4)));
                                case MONTH:
                                case YEAR_TO_MONTH:
                                default:
                                    throw new UnsupportedOperationException(
                                            "jdbc converter only support YEAR");
                            }
                        };
            case FLOAT:
                return val -> new FloatColumn((Float) val);
            case DOUBLE:
                return val -> new DoubleColumn((Double) val);
            case BIGINT:
                return val -> new LongColumn((Long) val);
            case DECIMAL:
                return val -> new BigDecimalColumn((BigDecimal) val);
            case CHAR:
            case VARCHAR:
                if (type instanceof PgCustomType && ((PgCustomType) type).isArray()) {
                    return val -> new StringColumn(val.toString());
                } else if (type instanceof JsonType
                        || type instanceof JsonbType
                        || type instanceof VarbitType
                        || type instanceof PointType) {
                    return val -> new StringColumn(((PGobject) val).getValue());
                } else if (type instanceof UuidType) {
                    return val -> new StringColumn(((UUID) val).toString());
                } else if (type instanceof XmlType) {
                    return val -> new StringColumn(((PgSQLXML) val).getString());
                }
                return val -> new StringColumn(val.toString());
            case DATE:
                return val -> new SqlDateColumn((Date) val);
            case TIME_WITHOUT_TIME_ZONE:
                return val -> new TimeColumn((Time) val);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val -> {
                            if (val instanceof PGobject) {
                                return new TimestampColumn(
                                        DateUtil.convertToTimestampWithZone(val.toString()), 0);
                            }

                            return new TimestampColumn(
                                    (Timestamp) val, ((TimestampType) (type)).getPrecision());
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
                PGobject pgObject = new PGobject();
                pgObject.setType("bit");
                if (type instanceof BitType) {
                    return (val, index, statement) -> {
                        pgObject.setValue(
                                ((ColumnRowData) val).getField(index).asBoolean() ? "1" : "0");
                        statement.setObject(index, pgObject);
                    };
                }
                return (val, index, statement) ->
                        statement.setBoolean(
                                index, ((ColumnRowData) val).getField(index).asBoolean());
            case INTEGER:
                return (val, index, statement) ->
                        statement.setInt(index, ((ColumnRowData) val).getField(index).asInt());
            case FLOAT:
                return (val, index, statement) ->
                        statement.setFloat(index, ((ColumnRowData) val).getField(index).asFloat());
            case DOUBLE:
                if (type instanceof MoneyType) {
                    PGobject pGobject = new PGobject();
                    pGobject.setType("money");
                    return (val, index, statement) -> {
                        pGobject.setValue(((ColumnRowData) val).getField(index).asString());
                        statement.setObject(index, pGobject);
                    };
                }
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
                if (type instanceof PgCustomType && ((PgCustomType) type).isArray()) {
                    final int oid = ((PgCustomType) type).getArrayOid();
                    return (val, index, statement) ->
                            statement.setArray(
                                    index,
                                    new PgArray(
                                            connection,
                                            oid,
                                            (String)
                                                    ((ColumnRowData) val)
                                                            .getField(index)
                                                            .getData()));
                } else if (type instanceof JsonType) {
                    PGobject jsonObject = new PGobject();
                    jsonObject.setType("json");
                    return (val, index, statement) -> {
                        jsonObject.setValue(((ColumnRowData) val).getField(index).asString());
                        statement.setObject(index, jsonObject);
                    };
                } else if (type instanceof JsonbType) {
                    PGobject jsonObject = new PGobject();
                    jsonObject.setType("jsonb");
                    return (val, index, statement) -> {
                        jsonObject.setValue(((ColumnRowData) val).getField(index).asString());
                        statement.setObject(index, jsonObject);
                    };
                } else if (type instanceof VarbitType) {
                    PGobject jsonObject = new PGobject();
                    jsonObject.setType("varbit");
                    return (val, index, statement) -> {
                        jsonObject.setValue(((ColumnRowData) val).getField(index).asString());
                        statement.setObject(index, jsonObject);
                    };
                } else if (type instanceof XmlType) {
                    PGobject jsonObject = new PGobject();
                    jsonObject.setType("xml");
                    return (val, index, statement) -> {
                        jsonObject.setValue(((ColumnRowData) val).getField(index).asString());
                        statement.setObject(index, jsonObject);
                    };
                } else if (type instanceof UuidType) {
                    PGobject jsonObject = new PGobject();
                    jsonObject.setType("uuid");
                    return (val, index, statement) -> {
                        jsonObject.setValue(((ColumnRowData) val).getField(index).asString());
                        statement.setObject(index, jsonObject);
                    };
                } else if (type instanceof PointType) {
                    PGobject jsonObject = new PGobject();
                    jsonObject.setType("point");
                    return (val, index, statement) -> {
                        jsonObject.setValue(((ColumnRowData) val).getField(index).asString());
                        statement.setObject(index, jsonObject);
                    };
                }
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

    public void setConnection(BaseConnection connection) {
        this.connection = connection;
    }
}
