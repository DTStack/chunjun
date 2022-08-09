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
import com.dtstack.chunjun.connector.inceptor.util.InceptorUtil;
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
import org.apache.flink.table.data.TimestampData;

import org.apache.hadoop.hive.common.type.HiveDate;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class InceptorParquetColumnConverter
        extends AbstractRowConverter<RowData, RowData, Group, String> {
    private List<String> columnNameList;
    private transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;

    public InceptorParquetColumnConverter(List<FieldConf> fieldConfList) {
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
            throw new ChunJunRuntimeException(
                    "Error RowData type, RowData:["
                            + input
                            + "] should be instance of GenericRowData.");
        }
        return row;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Group toExternal(RowData rowData, Group group) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters.get(index).serialize(rowData, index, group);
        }
        return group;
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new ChunJunRuntimeException("HDFS Connector doesn't support Lookup Table Function.");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<Group> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, String type) {
        return (rowData, index, group) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                // do nothing
            } else {
                serializationConverter.serialize(rowData, index, group);
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
            case "SMALLINT":
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
                        TimestampColumn::new;
            case "DATE":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new TimestampColumn(DateUtil.getTimestampFromStr(val));
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
    protected ISerializationConverter<Group> createExternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getBoolean(index));
            case "TINYINT":
            case "SMALLINT":
            case "INT":
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getInt(index));
            case "BIGINT":
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getLong(index));
            case "FLOAT":
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getFloat(index));
            case "DOUBLE":
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getDouble(index));
            case "DECIMAL":
                return (rowData, index, group) -> {
                    ColumnTypeUtil.DecimalInfo decimalInfo =
                            decimalColInfo.get(columnNameList.get(index));
                    HiveDecimal hiveDecimal =
                            HiveDecimal.create(
                                    rowData.getDecimal(
                                                    index,
                                                    decimalInfo.getPrecision(),
                                                    decimalInfo.getScale())
                                            .toBigDecimal());
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
                    group.add(
                            columnNameList.get(index),
                            InceptorUtil.decimalToBinary(
                                    hiveDecimal,
                                    decimalInfo.getPrecision(),
                                    decimalInfo.getScale()));
                };
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getString(index).toString());
            case "TIMESTAMP":
                return (rowData, index, group) -> {
                    TimestampData timestampData = rowData.getTimestamp(index, 6);
                    group.add(
                            columnNameList.get(index),
                            InceptorUtil.timestampToInt96(timestampData));
                };
            case "DATE":
                return (rowData, index, group) -> {
                    TimestampData timestampData = rowData.getTimestamp(index, 6);
                    Date date = Date.valueOf(timestampData.toLocalDateTime().toLocalDate());
                    group.add(
                            columnNameList.get(index),
                            DateWritable.dateToDays(new HiveDate(date.getTime())));
                };
            case "BINARY":
                return (rowData, index, group) ->
                        group.add(
                                columnNameList.get(index),
                                Binary.fromReusedByteArray(rowData.getBinary(index)));
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
