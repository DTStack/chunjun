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
import com.dtstack.chunjun.connector.hive3.util.Hive3Util;
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
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

public class HdfsParquetSyncConverter
        extends AbstractRowConverter<RowData, RowData, Group, LogicalType> {
    private static final long serialVersionUID = -7219261940027025732L;

    private final List<String> columnNameList;
    HdfsConfig hdfsConfig;

    public HdfsParquetSyncConverter(RowType rowType, HdfsConfig hdfsConfig) {
        super(rowType, hdfsConfig);
        this.hdfsConfig = hdfsConfig;
        List<FieldConfig> fieldConfList = hdfsConfig.getColumn();
        columnNameList =
                fieldConfList.stream().map(FieldConfig::getName).collect(Collectors.toList());
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
    protected ISerializationConverter<Group> wrapIntoNullableExternalConverter(
            ISerializationConverter<Group> serializationConverter, LogicalType type) {
        return (rowData, index, group) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                // do nothing
            } else {
                serializationConverter.serialize(rowData, index, group);
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
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
    @SuppressWarnings("unchecked")
    public Group toExternal(RowData rowData, Group group) throws Exception {
        for (int index = 0; index < hdfsConfig.getFullColumnName().size(); index++) {
            int columnIndex = hdfsConfig.getFullColumnIndexes()[index];
            if (columnIndex == -1) {
                continue;
            }
            toExternalConverters.get(columnIndex).serialize(rowData, columnIndex, group);
        }
        return group;
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new ChunJunRuntimeException("HDFS Connector doesn't support Lookup Table Function.");
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return (IDeserializationConverter<Boolean, AbstractBaseColumn>) BooleanColumn::new;
            case TINYINT:
                return (IDeserializationConverter<Integer, AbstractBaseColumn>)
                        val -> new ByteColumn(val.byteValue());
            case SMALLINT:
                return (IDeserializationConverter<Integer, AbstractBaseColumn>)
                        val -> new ShortColumn(val.shortValue());
            case INTEGER:
                return (IDeserializationConverter<Integer, AbstractBaseColumn>) IntColumn::new;
            case BIGINT:
                return (IDeserializationConverter<Long, AbstractBaseColumn>) LongColumn::new;
            case FLOAT:
                return (IDeserializationConverter<Float, AbstractBaseColumn>) FloatColumn::new;
            case DOUBLE:
                return (IDeserializationConverter<Double, AbstractBaseColumn>) DoubleColumn::new;
            case DECIMAL:
                return (IDeserializationConverter<BigDecimal, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case VARCHAR:
                return (IDeserializationConverter<String, AbstractBaseColumn>) StringColumn::new;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Timestamp, AbstractBaseColumn>)
                        TimestampColumn::new;
            case DATE:
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new TimestampColumn(DateUtil.getTimestampFromStr(val));
            case BINARY:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>) BytesColumn::new;
            default:
                throw new UnsupportedTypeException(logicalType);
        }
    }

    @Override
    protected ISerializationConverter<Group> createExternalConverter(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return (rowData, index, data) ->
                        data.add(
                                columnNameList.get(index),
                                ((ColumnRowData) rowData).getField(index).asBoolean());
            case TINYINT:
                return (rowData, index, data) ->
                        data.add(columnNameList.get(index), rowData.getByte(index));
            case SMALLINT:
            case INTEGER:
                return (rowData, index, data) ->
                        data.add(
                                columnNameList.get(index),
                                ((ColumnRowData) rowData).getField(index).asYearInt());

            case BIGINT:
                return (rowData, index, data) ->
                        data.add(
                                columnNameList.get(index),
                                ((ColumnRowData) rowData).getField(index).asLong());
            case FLOAT:
                return (rowData, index, data) ->
                        data.add(
                                columnNameList.get(index),
                                ((ColumnRowData) rowData).getField(index).asFloat());
            case DOUBLE:
                return (rowData, index, data) ->
                        data.add(
                                columnNameList.get(index),
                                ((ColumnRowData) rowData).getField(index).asDouble());
            case DECIMAL:
                return (rowData, index, data) -> {
                    DecimalType decimalType = (DecimalType) logicalType;
                    int precision = decimalType.getPrecision();
                    int scale = decimalType.getScale();
                    HiveDecimal hiveDecimal =
                            HiveDecimal.create(
                                    rowData.getDecimal(index, precision, scale).toBigDecimal());
                    hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, precision, scale);
                    if (hiveDecimal == null) {
                        String msg =
                                String.format(
                                        "The [%s] data data [%s] precision and scale do not match the metadata:decimal(%s, %s)",
                                        index, precision, scale, rowData);
                        throw new WriteRecordException(msg, new IllegalArgumentException());
                    }
                    data.add(
                            columnNameList.get(index),
                            Hive3Util.decimalToBinary(hiveDecimal, precision, scale));
                };
            case VARCHAR:
                return (rowData, index, data) ->
                        data.add(
                                columnNameList.get(index),
                                ((ColumnRowData) rowData).getField(index).asString());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (rowData, index, data) -> {
                    Timestamp timestamp = ((ColumnRowData) rowData).getField(index).asTimestamp();
                    data.add(columnNameList.get(index), Hive3Util.timeToBinary(timestamp));
                };
            case DATE:
                return (rowData, index, data) -> {
                    TimestampData timestampData = rowData.getTimestamp(index, 6);
                    Date date = Date.valueOf(timestampData.toLocalDateTime().toLocalDate());
                    data.add(columnNameList.get(index), DateWritable.dateToDays(date));
                };
            case BINARY:
                return (rowData, index, data) ->
                        data.add(
                                columnNameList.get(index),
                                Binary.fromReusedByteArray(
                                        ((ColumnRowData) rowData).getField(index).asBytes()));
            default:
                throw new UnsupportedTypeException(logicalType);
        }
    }
}
