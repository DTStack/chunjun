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

package com.dtstack.flinkx.connector.jdbc.converter;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
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
import com.dtstack.flinkx.util.DateUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

/** Base class for all converters that convert between JDBC object and Flink internal object. */
public class JdbcColumnConverter
        extends AbstractRowConverter<
                ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType> {

    public JdbcColumnConverter(RowType rowType) {
        this(rowType, null);
    }

    public JdbcColumnConverter(RowType rowType, FlinkxCommonConf commonConf) {
        super(rowType, commonConf);
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
                    ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, statement) -> {
            if (((ColumnRowData) val).getField(index) == null) {
                statement.setObject(index, null);
            } else {
                serializationConverter.serialize(val, index, statement);
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(ResultSet resultSet) throws Exception {
        List<FieldConf> fieldConfList = commonConf.getColumn();
        ColumnRowData result = new ColumnRowData(fieldConfList.size());
        int converterIndex = 0;
        for (FieldConf fieldConf : fieldConfList) {
            AbstractBaseColumn baseColumn = null;
            if (StringUtils.isBlank(fieldConf.getValue())) {
                Object field = resultSet.getObject(converterIndex + 1);
                baseColumn =
                        (AbstractBaseColumn)
                                toInternalConverters.get(converterIndex).deserialize(field);
                converterIndex++;
            }
            result.addField(assembleFieldProps(fieldConf, baseColumn));
        }
        return result;
    }

    @Override
    public FieldNamedPreparedStatement toExternal(
            RowData rowData, FieldNamedPreparedStatement statement) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
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
                return val -> new StringColumn((String) val);
            case INTERVAL_YEAR_MONTH:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val -> {
                            Timestamp timestamp = DateUtil.convertToTimestamp(val.toString());
                            if (timestamp != null) {
                                return new TimestampColumn(timestamp);
                            }
                            return new TimestampColumn(
                                    DateUtil.getTimestampFromStr(val.toString()));
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
                return (val, index, statement) ->
                        statement.setString(
                                index, ((ColumnRowData) val).getField(index).asString());
            case INTERVAL_YEAR_MONTH:
                return (val, index, statement) ->
                        statement.setInt(
                                index,
                                ((ColumnRowData) val)
                                        .getField(index)
                                        .asTimestamp()
                                        .toLocalDateTime()
                                        .toLocalDate()
                                        .getYear());
            case DATE:
                return (val, index, statement) ->
                        statement.setDate(
                                index,
                                Date.valueOf(
                                        ((ColumnRowData) val)
                                                .getField(index)
                                                .asTimestamp()
                                                .toLocalDateTime()
                                                .toLocalDate()));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTime(
                                index,
                                Time.valueOf(
                                        ((ColumnRowData) val)
                                                .getField(index)
                                                .asTimestamp()
                                                .toLocalDateTime()
                                                .toLocalTime()));
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
}
