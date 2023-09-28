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
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@Slf4j
public class HdfsTextSyncConverter
        extends AbstractRowConverter<RowData, RowData, String[], String> {

    private static final long serialVersionUID = 1191849197062775272L;

    private List<String> columnNameList;

    public HdfsTextSyncConverter(List<FieldConfig> fieldConfigList, HdfsConfig hdfsConfig) {
        super(fieldConfigList.size(), hdfsConfig);
        columnNameList =
                hdfsConfig.getColumn().stream()
                        .map(FieldConfig::getName)
                        .collect(Collectors.toList());
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
            List<FieldConfig> fieldConfigList = commonConfig.getColumn();
            for (int i = 0; i < input.getArity(); i++) {
                AbstractBaseColumn baseColumn =
                        (AbstractBaseColumn)
                                toInternalConverters.get(i).deserialize(genericRowData.getField(i));
                if (baseColumn != null) {
                    row.addField(assembleFieldProps(fieldConfigList.get(i), baseColumn));
                } else {
                    row.addField(new NullColumn());
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
    @SuppressWarnings("unchecked")
    public String[] toExternal(RowData rowData, String[] data) throws Exception {
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
    protected IDeserializationConverter wrapIntoNullableInternalConverter(
            IDeserializationConverter IDeserializationConverter) {
        return val -> {
            if (val == null || "".equals(val)) {
                return null;
            } else {
                try {
                    return IDeserializationConverter.deserialize(val);
                } catch (Exception e) {
                    log.error("value [{}] convent failed ", val);
                    throw e;
                }
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<String[]> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, String type) {
        return (rowData, index, data) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                data[index] = "\\N";
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
                return (IDeserializationConverter<Byte, AbstractBaseColumn>) ByteColumn::new;
            case "SMALLINT":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new ShortColumn(Short.parseShort(val));
            case "INT":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new IntColumn(Integer.parseInt(val));
            case "BIGINT":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new LongColumn(Long.parseLong(val));
            case "FLOAT":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new FloatColumn(Float.parseFloat(val));
            case "DOUBLE":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new DoubleColumn(Double.parseDouble(val));
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
                        val ->
                                val == null
                                        ? new SqlDateColumn(null)
                                        : new SqlDateColumn(Date.valueOf(val));
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
