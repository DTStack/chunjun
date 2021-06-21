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

package com.dtstack.flinkx.connector.cassandra.converter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraRowConverter
        extends AbstractRowConverter<ResultSet, ResultSet, BoundStatement, LogicalType> {

    private static final long serialVersionUID = 1L;

    public CassandraRowConverter(RowType rowType) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters[i] =
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i)));
            toExternalConverters[i] =
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]);
        }
    }

    @Override
    // 将数据库中的数据类型转化为flink的数据类型 input -> row data
    public RowData toInternal(ResultSet input) throws Exception {
        ColumnRowData columnRowData = new ColumnRowData(toInternalConverters.length);

        Row row = input.one();

        for (int i = 0; i < toInternalConverters.length; i++) {
            final Object value = row.getObject(i);
            columnRowData.setField(
                    i, (AbstractBaseColumn) toInternalConverters[i].deserialize(value));
        }
        return columnRowData;
    }

    @Override
    public BoundStatement toExternal(RowData rowData, BoundStatement statement) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, statement);
        }
        return statement;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return null;
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(String.valueOf(val)));
            case TINYINT:
                return val -> new BigDecimalColumn(((Integer) val).byteValue());
            case SMALLINT:
            case INTEGER:
                return val -> new BigDecimalColumn((Integer) val);
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
                return val -> new StringColumn(String.valueOf(val));
            case DATE:
                return val ->
                        new BigDecimalColumn(
                                Date.valueOf(String.valueOf(val)).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        new BigDecimalColumn(
                                Time.valueOf(String.valueOf(val)).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> new TimestampColumn((Timestamp) val);
            case BINARY:
            case VARBINARY:
                return val -> new BytesColumn((byte[]) val);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<BoundStatement> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) ->
                        statement.setBool(index, ((ColumnRowData) val).getField(index).asBoolean());
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
                        statement.setDecimal(
                                index, ((ColumnRowData) val).getField(index).asBigDecimal());
            case CHAR:
            case VARCHAR:
                return (val, index, statement) ->
                        statement.setString(
                                index, ((ColumnRowData) val).getField(index).asString());
            case DATE:
                return (val, index, statement) ->
                        statement.setDate(
                                index,
                                com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(
                                        ((ColumnRowData) val).getField(index).asInt()));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTime(
                                index, ((ColumnRowData) val).getField(index).asInt() * 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, ((ColumnRowData) val).getField(index).asTimestamp());

            case BINARY:
            case VARBINARY:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
