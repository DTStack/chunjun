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

package com.dtstack.chunjun.connector.hive3.converter;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hive3.config.HdfsConfig;
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
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.hive.common.type.HiveDecimal;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class HdfsTextSyncConverter
        extends AbstractRowConverter<RowData, RowData, List<String>, LogicalType> {

    private static final long serialVersionUID = -6245541661525501806L;

    HdfsConfig hdfsConfig;

    public HdfsTextSyncConverter(RowType rowType, HdfsConfig hdfsConfig) {
        super(rowType, hdfsConfig);
        this.hdfsConfig = hdfsConfig;
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
    public RowData toInternal(RowData input) throws Exception {
        ColumnRowData row = new ColumnRowData(input.getArity());
        if (input instanceof GenericRowData) {
            List<FieldConfig> fieldConfList = commonConfig.getColumn();
            GenericRowData genericRowData = (GenericRowData) input;
            for (int i = 0; i < fieldConfList.size(); i++) {
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
    public List<String> toExternal(RowData rowData, List<String> output) throws Exception {
        for (int index = 0; index < hdfsConfig.getFullColumnName().size(); index++) {
            int columnIndex = hdfsConfig.getFullColumnIndexes()[index];
            if (columnIndex == -1) {
                output.add("");
            } else {
                toExternalConverters.get(columnIndex).serialize(rowData, columnIndex, output);
            }
        }
        return output;
    }

    @Override
    protected ISerializationConverter<List<String>> wrapIntoNullableExternalConverter(
            ISerializationConverter<List<String>> serializationConverter, LogicalType type) {
        return (rowData, index, data) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                data.add(null);
            } else {
                serializationConverter.serialize(rowData, index, data);
            }
        };
    }

    @Override
    protected IDeserializationConverter<String, AbstractBaseColumn> createInternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val));
            case TINYINT:
                return val -> new ByteColumn(Byte.parseByte(val));
            case SMALLINT:
                return val -> new ShortColumn(Short.parseShort(val));
            case INTEGER:
                return val -> new IntColumn(Integer.parseInt(val));
            case BIGINT:
                return val -> new LongColumn(Long.parseLong(val));
            case FLOAT:
                return val -> new FloatColumn(Float.parseFloat(val));
            case DOUBLE:
                return val -> new DoubleColumn(Double.parseDouble(val));
            case DECIMAL:
                return BigDecimalColumn::new;
            case VARCHAR:
                return StringColumn::new;
            case BINARY:
                return val -> new BytesColumn(val.getBytes(StandardCharsets.UTF_8));
            case DATE:
                return val -> {
                    Timestamp timestamp = DateUtil.getTimestampFromStr(val);
                    if (timestamp == null) {
                        return new SqlDateColumn(null);
                    } else {
                        return new SqlDateColumn(
                                Date.valueOf(timestamp.toLocalDateTime().toLocalDate()));
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    try {
                        return new TimestampColumn(
                                Timestamp.valueOf(val), DateUtil.getPrecisionFromTimestampStr(val));
                    } catch (Exception e) {
                        return new TimestampColumn(DateUtil.getTimestampFromStr(val), 0);
                    }
                };
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    @Override
    protected ISerializationConverter<List<String>> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (rowData, index, data) ->
                        data.add(
                                String.valueOf(
                                        ((ColumnRowData) rowData).getField(index).asBoolean()));
            case TINYINT:
                return (rowData, index, data) -> data.add(String.valueOf(rowData.getByte(index)));
            case SMALLINT:
            case INTEGER:
                return (rowData, index, data) ->
                        data.add(
                                String.valueOf(
                                        ((ColumnRowData) rowData).getField(index).asYearInt()));
            case BIGINT:
                return (rowData, index, data) ->
                        data.add(
                                String.valueOf(((ColumnRowData) rowData).getField(index).asLong()));
            case FLOAT:
                return (rowData, index, data) ->
                        data.add(
                                String.valueOf(
                                        ((ColumnRowData) rowData).getField(index).asFloat()));
            case DOUBLE:
                return (rowData, index, data) ->
                        data.add(
                                String.valueOf(
                                        ((ColumnRowData) rowData).getField(index).asDouble()));
            case DECIMAL:
                return (rowData, index, data) ->
                        data.add(
                                String.valueOf(
                                        HiveDecimal.create(
                                                ((ColumnRowData) rowData)
                                                        .getField(index)
                                                        .asBigDecimal())));
            case VARCHAR:
                return (rowData, index, data) ->
                        data.add(
                                String.valueOf(
                                        ((ColumnRowData) rowData).getField(index).asString()));
            case DATE:
                return (rowData, index, data) ->
                        data.add(
                                String.valueOf(
                                        ((ColumnRowData) rowData).getField(index).asSqlDate()));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (rowData, index, data) ->
                        data.add(((ColumnRowData) rowData).getField(index).asTimestampStr());
            case BINARY:
                return (rowData, index, data) ->
                        data.add(
                                Arrays.toString(
                                        ((ColumnRowData) rowData).getField(index).asBytes()));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
