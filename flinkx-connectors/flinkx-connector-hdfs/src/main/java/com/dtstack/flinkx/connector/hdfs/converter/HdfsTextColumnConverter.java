/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.connector.hdfs.converter;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.SqlDateColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;
import com.dtstack.flinkx.util.DateUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Date: 2021/06/16 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsTextColumnConverter
        extends AbstractRowConverter<RowData, RowData, String[], String> {

    public HdfsTextColumnConverter(List<FieldConf> fieldConfList) {
        super(fieldConfList.size());
        for (int i = 0; i < fieldConfList.size(); i++) {
            String type = fieldConfList.get(i).getType();
            int left = type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
            int right = type.indexOf(ConstantValue.RIGHT_PARENTHESIS_SYMBOL);
            if (left > 0 && right > 0) {
                type = type.substring(0, left);
            }
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(createInternalConverter(type)));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(createExternalConverter(type), type));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(RowData input) throws Exception {
        ColumnRowData row = new ColumnRowData(input.getArity());
        if (input instanceof GenericRowData) {
            GenericRowData genericRowData = (GenericRowData) input;
            for (int i = 0; i < input.getArity(); i++) {
                row.addField(
                        (AbstractBaseColumn)
                                toInternalConverters
                                        .get(i)
                                        .deserialize(genericRowData.getField(i)));
            }
        } else {
            throw new FlinkxRuntimeException(
                    "Error RowData type, RowData:["
                            + input
                            + "] should be instance of GenericRowData.");
        }
        return row;
    }

    @Override
    @SuppressWarnings("unchecked")
    public String[] toExternal(RowData rowData, String[] data) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters.get(index).serialize(rowData, index, data);
        }
        return data;
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new FlinkxRuntimeException("HDFS Connector doesn't support Lookup Table Function.");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<String[]> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, String type) {
        return (rowData, index, data) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                data[index] = null;
            } else {
                serializationConverter.serialize(rowData, index, data);
            }
        };
    }

    @Override
    protected IDeserializationConverter createInternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new BooleanColumn(Boolean.parseBoolean(val));
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "BIGINT":
            case "FLOAT":
            case "DOUBLE":
            case "DECIMAL":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return (IDeserializationConverter<String, AbstractBaseColumn>) StringColumn::new;
            case "TIMESTAMP":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> {
                            try {
                                return new TimestampColumn(
                                        Timestamp.valueOf(val),
                                        DateUtil.getPrecisionFromTimestampStr(val));
                            } catch (Exception e) {
                                return new TimestampColumn(DateUtil.getTimestampFromStr(val), 0);
                            }
                        };
            case "DATE":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> {
                            Timestamp timestamp = DateUtil.getTimestampFromStr(val);
                            if (timestamp == null) {
                                return new SqlDateColumn(null);
                            } else {
                                return new SqlDateColumn(
                                        Date.valueOf(timestamp.toLocalDateTime().toLocalDate()));
                            }
                        };
            case "BINARY":
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    @Override
    protected ISerializationConverter<String[]> createExternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getBoolean(index));
            case "TINYINT":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getByte(index));
            case "SMALLINT":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getShort(index));
            case "INT":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getInt(index));
            case "BIGINT":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getLong(index));
            case "FLOAT":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getFloat(index));
            case "DOUBLE":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getDouble(index));
            case "DECIMAL":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getDecimal(index, 38, 18));
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return (rowData, index, data) ->
                        data[index] = String.valueOf(rowData.getString(index));
            case "TIMESTAMP":
                return (rowData, index, data) -> {
                    AbstractBaseColumn field = ((ColumnRowData) rowData).getField(index);
                    data[index] = field.asTimestampStr();
                };
            case "DATE":
                return (rowData, index, data) ->
                        data[index] =
                                String.valueOf(
                                        new Date(rowData.getTimestamp(index, 6).getMillisecond()));
            case "BINARY":
                return (rowData, index, data) ->
                        data[index] = Arrays.toString(rowData.getBinary(index));
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
