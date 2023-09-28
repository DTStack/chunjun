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
package com.dtstack.chunjun.connector.hdfs.converter;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hdfs.config.HdfsConfig;
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
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ColumnTypeUtil;
import com.dtstack.chunjun.util.DateUtil;

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

public class HdfsOrcSyncConverter extends AbstractRowConverter<RowData, RowData, Object[], String> {

    private static final long serialVersionUID = 4254984437380862131L;

    private List<String> columnNameList;
    private transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;

    public HdfsOrcSyncConverter(List<FieldConfig> fieldConfigList, HdfsConfig hdfsConfig) {
        super(fieldConfigList.size(), hdfsConfig);
        for (FieldConfig fieldConfig : fieldConfigList) {
            String type = fieldConfig.getType().getType();
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
            List<FieldConfig> fieldConfList = commonConfig.getColumn();
            for (int i = 0; i < input.getArity(); i++) {
                row.addField(
                        assembleFieldProps(
                                fieldConfList.get(i),
                                (AbstractBaseColumn)
                                        toInternalConverters
                                                .get(i)
                                                .deserialize(genericRowData.getField(i))));
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
    @SuppressWarnings("unchecked")
    public Object[] toExternal(RowData rowData, Object[] data) throws Exception {
        for (int index = 0; index < columnNameList.size(); index++) {
            toExternalConverters.get(index).serialize(rowData, index, data);
        }
        return data;
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new ChunJunRuntimeException("HDFS Connector doesn't support Lookup Table Function.");
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
                return (IDeserializationConverter<Byte, AbstractBaseColumn>) ByteColumn::new;
            case "SMALLINT":
                return (IDeserializationConverter<Short, AbstractBaseColumn>) ShortColumn::new;
            case "INT":
                return (IDeserializationConverter<Integer, AbstractBaseColumn>) IntColumn::new;
            case "BIGINT":
                return (IDeserializationConverter<Long, AbstractBaseColumn>) LongColumn::new;
            case "FLOAT":
                return (IDeserializationConverter<Float, AbstractBaseColumn>) FloatColumn::new;
            case "DOUBLE":
                return (IDeserializationConverter<Double, AbstractBaseColumn>) DoubleColumn::new;
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
                            decimalColInfo.get(columnNameList.get(index));
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
                return (rowData, index, data) -> {
                    Timestamp ts = rowData.getTimestamp(index, 6).toTimestamp();
                    int nanos = ts.getNanos();
                    data[index] =
                            org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(
                                    ts.getTime(), nanos);
                };
            case "DATE":
                return (rowData, index, data) ->
                        data[index] =
                                org.apache.hadoop.hive.common.type.Date.ofEpochMilli(
                                        rowData.getTimestamp(index, 6).getMillisecond());
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
        this.columnNameList = columnNameList;
    }

    public void setDecimalColInfo(Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo) {
        this.decimalColInfo = decimalColInfo;
    }
}
