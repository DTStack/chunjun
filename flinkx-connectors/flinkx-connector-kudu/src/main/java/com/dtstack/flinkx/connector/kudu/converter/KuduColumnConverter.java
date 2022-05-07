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

package com.dtstack.flinkx.connector.kudu.converter;

import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.ByteColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.SqlDateColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.kudu.client.Operation;
import org.apache.kudu.client.RowResult;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;

/**
 * @author tiezhu
 * @since 2021/6/10 星期四
 */
public class KuduColumnConverter
        extends AbstractRowConverter<RowResult, RowResult, Operation, String> {

    private static final long serialVersionUID = 1L;

    private final List<String> columnName;

    public KuduColumnConverter(RowType rowType, List<String> columnName) {
        super(rowType);
        this.columnName = columnName;
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i).getTypeRoot().name())));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(rowType.getTypeAt(i).getTypeRoot().name()),
                            rowType.getTypeAt(i).getTypeRoot().name()));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<Operation> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, String type) {
        return (val, index, operation) -> {
            if (((ColumnRowData) val).getField(index) == null || val.isNullAt(index)) {
                operation.getRow().setNull(columnName.get(index));
            } else {
                serializationConverter.serialize(val, index, operation);
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(RowResult input) throws Exception {
        ColumnRowData data = new ColumnRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = input.getObject(pos);
            data.addField((AbstractBaseColumn) toInternalConverters.get(pos).deserialize(field));
        }
        return data;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Operation toExternal(RowData rowData, Operation operation) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters.get(index).serialize(rowData, index, operation);
        }
        return operation;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOL":
            case "BOOLEAN":
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case "INT8":
            case "TINYINT":
                return val -> new ByteColumn((byte) val);
            case "INT16":
            case "SMALLINT":
                return val -> new BigDecimalColumn((Short) val);
            case "INTEGER":
            case "INT32":
            case "INT":
                return val -> new BigDecimalColumn((Integer) val);
            case "FLOAT":
                return val -> new BigDecimalColumn((Float) val);
            case "DOUBLE":
                return val -> new BigDecimalColumn((Double) val);
            case "LONG":
            case "INT64":
            case "BIGINT":
                return val -> new BigDecimalColumn((Long) val);
            case "DECIMAL":
                return val -> new BigDecimalColumn((BigDecimal) val);
            case "VARCHAR":
            case "STRING":
                return val -> new StringColumn((String) val);
            case "DATE":
                return val -> new SqlDateColumn(Date.valueOf(String.valueOf(val)));
            case "TIMESTAMP":
                return val -> new TimestampColumn((Timestamp) val);
            case "BINARY":
                return val -> new BytesColumn((byte[]) val);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<Operation> createExternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
            case "BOOL":
                return (val, index, operation) ->
                        operation.getRow().addBoolean(columnName.get(index), val.getBoolean(index));
            case "TINYINT":
            case "CHAR":
            case "INT8":
                return (val, index, operation) ->
                        operation.getRow().addByte(columnName.get(index), val.getByte(index));
            case "INT16":
            case "SMALLINT":
                return (val, index, operation) ->
                        operation.getRow().addShort(columnName.get(index), val.getShort(index));
            case "INTEGER":
            case "INT":
            case "INT32":
                return (val, index, operation) ->
                        operation.getRow().addInt(columnName.get(index), val.getInt(index));
            case "BIGINT":
            case "INT64":
                return (val, index, operator) ->
                        operator.getRow().addLong(columnName.get(index), val.getLong(index));
            case "FLOAT":
                return (val, index, operation) ->
                        operation.getRow().addFloat(columnName.get(index), val.getFloat(index));
            case "DOUBLE":
                return (val, index, operation) ->
                        operation.getRow().addDouble(columnName.get(index), val.getDouble(index));
            case "BINARY":
                return (val, index, operation) ->
                        operation.getRow().addBinary(columnName.get(index), val.getBinary(index));
            case "DECIMAL":
                return (val, index, operation) ->
                        operation
                                .getRow()
                                .addDecimal(
                                        columnName.get(index),
                                        ((ColumnRowData) val).getField(index).asBigDecimal());
            case "VARCHAR":
                return (val, index, operation) ->
                        operation
                                .getRow()
                                .addString(columnName.get(index), val.getString(index).toString());
            case "DATE":
                return (val, index, operation) ->
                        operation
                                .getRow()
                                .addDate(
                                        columnName.get(index),
                                        ((ColumnRowData) val).getField(index).asSqlDate());

            case "TIMESTAMP":
                return (val, index, operation) ->
                        operation
                                .getRow()
                                .addTimestamp(
                                        columnName.get(index),
                                        ((ColumnRowData) val).getField(index).asTimestamp());
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
