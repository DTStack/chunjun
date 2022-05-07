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
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;
import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.ColumnTypeUtil;
import com.dtstack.flinkx.util.DateUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Date: 2021/06/16 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsOrcColumnConverter
        extends AbstractRowConverter<RowData, RowData, Object[], String> {

    private List<String> ColumnNameList;
    private transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;

    public HdfsOrcColumnConverter(List<FieldConf> fieldConfList) {
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
    public Object[] toExternal(RowData rowData, Object[] data) throws Exception {
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
                return (IDeserializationConverter<Boolean, AbstractBaseColumn>) BooleanColumn::new;
            case "TINYINT":
                return (IDeserializationConverter<Byte, AbstractBaseColumn>)
                        val -> new BigDecimalColumn(val.toString());
            case "SMALLINT":
                return (IDeserializationConverter<Short, AbstractBaseColumn>)
                        val -> new BigDecimalColumn(val.toString());
            case "INT":
                return (IDeserializationConverter<Integer, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "BIGINT":
                return (IDeserializationConverter<Long, AbstractBaseColumn>) BigDecimalColumn::new;
            case "FLOAT":
                return (IDeserializationConverter<Float, AbstractBaseColumn>) BigDecimalColumn::new;
            case "DOUBLE":
                return (IDeserializationConverter<Double, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "DECIMAL":
                return (IDeserializationConverter<BigDecimal, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return (IDeserializationConverter<String, AbstractBaseColumn>) StringColumn::new;
            case "TIMESTAMP":
                return (IDeserializationConverter<Timestamp, AbstractBaseColumn>)
                        val ->
                                new TimestampColumn(
                                        val, DateUtil.getPrecisionFromTimestampStr(val.toString()));
            case "DATE":
                return (IDeserializationConverter<Date, AbstractBaseColumn>)
                        val -> new TimestampColumn(val.getTime());
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
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (rowData, index, data) -> data[index] = rowData.getBoolean(index);
            case "TINYINT":
                return (rowData, index, data) -> data[index] = rowData.getByte(index);
            case "SMALLINT":
                return (rowData, index, data) -> data[index] = rowData.getShort(index);
            case "INT":
                return (rowData, index, data) -> data[index] = rowData.getInt(index);
            case "BIGINT":
                return (rowData, index, data) -> data[index] = rowData.getLong(index);
            case "FLOAT":
                return (rowData, index, data) -> data[index] = rowData.getFloat(index);
            case "DOUBLE":
                return (rowData, index, data) -> data[index] = rowData.getDouble(index);
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
            case "VARCHAR":
            case "CHAR":
                return (rowData, index, data) -> data[index] = rowData.getString(index).toString();
            case "TIMESTAMP":
                return (rowData, index, data) ->
                        data[index] = rowData.getTimestamp(index, 6).toTimestamp();
            case "DATE":
                return (rowData, index, data) ->
                        data[index] = new Date(rowData.getTimestamp(index, 6).getMillisecond());
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
}
