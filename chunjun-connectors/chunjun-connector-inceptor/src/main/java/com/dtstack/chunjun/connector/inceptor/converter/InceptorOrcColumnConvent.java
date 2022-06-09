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

package com.dtstack.chunjun.connector.inceptor.converter;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ColumnTypeUtil;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.hadoop.hive.common.Dialect;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarchar2Writable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.BytesWritable;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class InceptorOrcColumnConvent
        extends AbstractRowConverter<RowData, RowData, Object[], String> {

    private List<String> ColumnNameList;
    private transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;

    private boolean columnIsStarSymbol;

    public InceptorOrcColumnConvent(List<FieldConf> fieldConfList) {
        super(fieldConfList.size());
        columnIsStarSymbol =
                fieldConfList.size() == 1
                        && ConstantValue.STAR_SYMBOL.equals(fieldConfList.get(0).getName());
        if (!columnIsStarSymbol) {
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
    }

    @Override
    public RowData toInternal(RowData input) throws Exception {
        ColumnRowData row = new ColumnRowData(input.getArity());
        if (input instanceof GenericRowData) {
            GenericRowData genericRowData = (GenericRowData) input;
            if (columnIsStarSymbol) {
                for (int i = 0; i < input.getArity(); i++) {
                    if (genericRowData.getField(i) == null) {
                        row.addField(null);
                    } else {
                        row.addField(new StringColumn(genericRowData.getField(i).toString()));
                    }
                }
            } else {
                for (int i = 0; i < input.getArity(); i++) {
                    row.addField(
                            (AbstractBaseColumn)
                                    toInternalConverters
                                            .get(i)
                                            .deserialize(genericRowData.getField(i)));
                }
            }

        } else {
            throw new ChunJunRuntimeException(
                    "Error RowData type, RowData:["
                            + input
                            + "] should be instance of GenericRowData.");
        }
        return row;
    }

    @Override
    public Object[] toExternal(RowData rowData, Object[] output) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters.get(index).serialize(rowData, index, output);
        }
        return output;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<Object[]> wrapIntoNullableExternalConverter(
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
    @SuppressWarnings("all")
    protected IDeserializationConverter createInternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return val -> new BooleanColumn(Boolean.valueOf(val.toString()));
            case "TINYINT":
                return val -> new BigDecimalColumn(val.toString());
            case "SMALLINT":
                return val -> new BigDecimalColumn(val.toString());
            case "INT":
                return val -> new BigDecimalColumn(val.toString());
            case "BIGINT":
                return val -> new BigDecimalColumn(val.toString());
            case "FLOAT":
                return val -> new BigDecimalColumn(val.toString());
            case "DOUBLE":
                return val -> new BigDecimalColumn(val.toString());
            case "DECIMAL":
                return val -> new BigDecimalColumn(val.toString());
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return val -> new StringColumn(val.toString());
            case "TIMESTAMP":
                return (IDeserializationConverter<Timestamp, AbstractBaseColumn>)
                        val ->
                                new TimestampColumn(
                                        val, DateUtil.getPrecisionFromTimestampStr(val.toString()));
            case "DATE":
                return val -> {
                    if (val instanceof java.util.Date) {
                        return new TimestampColumn(((java.util.Date) val).getTime());
                    } else {
                        return new TimestampColumn(((java.sql.Date) val).getTime());
                    }
                };
            case "BINARY":
                return (IDeserializationConverter<byte[], AbstractBaseColumn>) BytesColumn::new;
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    @Override
    protected ISerializationConverter<Object[]> createExternalConverter(String type) {
        String previousType = type;
        if (type.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)) {
            type = type.substring(0, type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL));
        }

        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (rowData, index, data) -> data[index] = rowData.getBoolean(index);
            case "TINYINT":
                return (rowData, index, data) ->
                        data[index] = new ByteWritable(rowData.getByte(index));
            case "SMALLINT":
                return (rowData, index, data) ->
                        data[index] = new ShortWritable(rowData.getShort(index));
            case "INT":
                return (rowData, index, data) -> data[index] = rowData.getInt(index);
            case "BIGINT":
                return (rowData, index, data) -> data[index] = rowData.getLong(index);
            case "FLOAT":
                return (rowData, index, data) -> data[index] = rowData.getFloat(index);
            case "DOUBLE":
                return (rowData, index, data) ->
                        data[index] = new DoubleWritable(rowData.getDouble(index));
            case "DECIMAL":
                return (rowData, index, data) -> {
                    ColumnTypeUtil.DecimalInfo decimalInfo =
                            decimalColInfo.get(ColumnNameList.get(index));
                    HiveDecimal hiveDecimal =
                            HiveDecimal.create(new BigDecimal(rowData.getString(index).toString()));
                    hiveDecimal =
                            HiveDecimal.enforcePrecisionScale(
                                    hiveDecimal,
                                    decimalInfo.getPrecision(),
                                    decimalInfo.getScale());
                    if (hiveDecimal == null) {
                        String msg =
                                String.format(
                                        "The [%s] data data [%s] precision and scale do not match the metadata:decimal(%s, %s)",
                                        index,
                                        decimalInfo.getPrecision(),
                                        decimalInfo.getScale(),
                                        rowData);
                        throw new WriteRecordException(msg, new IllegalArgumentException());
                    }
                    data[index] = new HiveDecimalWritable(hiveDecimal);
                };
            case "STRING":
                return (rowData, index, data) -> {
                    if (rowData instanceof ColumnRowData) {
                        Object columnData = ((ColumnRowData) rowData).getField(index).getData();
                        if (columnData instanceof Timestamp) {
                            SimpleDateFormat fm = DateUtil.getDateTimeFormatterForMillisencond();
                            data[index] = fm.format(columnData);
                        } else {
                            data[index] = rowData.getString(index).toString();
                        }
                    } else {
                        data[index] = rowData.getString(index).toString();
                    }
                };

            case "VARCHAR":
                return (rowData, index, data) -> {
                    HiveVarchar hiveVarchar =
                            new HiveVarchar(
                                    rowData.getString(index).toString(),
                                    getVarcharLength(previousType));
                    HiveVarcharWritable hiveVarcharWritable = new HiveVarcharWritable(hiveVarchar);
                    data[index] = hiveVarcharWritable;
                };
            case "VARCHAR2":
                return (rowData, index, data) ->
                        data[index] =
                                HiveVarchar2Writable.createInstance(
                                        Dialect.UNKNOWN,
                                        getVarcharLength(previousType),
                                        rowData.getString(index).toString());
            case "TIMESTAMP":
                return (rowData, index, data) ->
                        data[index] = rowData.getTimestamp(index, 6).toTimestamp();
            case "DATE":
                return (rowData, index, data) ->
                        data[index] =
                                new DateWritable(rowData.getTimestamp(index, 6).getMillisecond());
            case "BINARY":
                return (rowData, index, data) ->
                        data[index] = new BytesWritable(rowData.getBinary(index));
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.ColumnNameList = columnNameList;
    }

    public void setDecimalColInfo(Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo) {
        this.decimalColInfo = decimalColInfo;
    }

    private int getVarcharLength(String columnName) {
        if (columnName.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)) {
            return Integer.parseInt(
                    columnName.substring(
                            columnName.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL) + 1,
                            columnName.indexOf(ConstantValue.RIGHT_PARENTHESIS_SYMBOL)));
        }
        return -1;
    }
}
