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
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

public class HdfsOrcSyncConverter
        extends AbstractRowConverter<RowData, RowData, List<Object>, LogicalType> {

    private static final long serialVersionUID = -1143450655293766846L;

    HdfsConfig hdfsConfig;

    public HdfsOrcSyncConverter(RowType rowType, HdfsConfig hdfsConfig) {
        super(rowType, hdfsConfig);
        this.hdfsConfig = hdfsConfig;
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(rowType.getTypeAt(i)), rowType.getTypeAt(i)));
        }
    }

    @Override
    protected ISerializationConverter<List<Object>> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (rowData, index, data) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                data.add(null);
            } else {
                serializationConverter.serialize(rowData, index, data);
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
    public RowData toInternalLookup(RowData input) {
        throw new ChunJunRuntimeException("HDFS Connector doesn't support Lookup Table Function.");
    }

    @Override
    public List<Object> toExternal(RowData rowData, List<Object> output) throws Exception {
        for (int index = 0; index < hdfsConfig.getFullColumnName().size(); index++) {
            int columnIndex = hdfsConfig.getFullColumnIndexes()[index];
            if (columnIndex == -1) {
                output.add(null);
                continue;
            }
            toExternalConverters.get(columnIndex).serialize(rowData, columnIndex, output);
        }
        return output;
    }

    @Override
    @SuppressWarnings("all")
    protected IDeserializationConverter createInternalConverter(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return (IDeserializationConverter<Boolean, AbstractBaseColumn>) BooleanColumn::new;
            case TINYINT:
                return (IDeserializationConverter<Byte, AbstractBaseColumn>) ByteColumn::new;
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
                return (IDeserializationConverter<
                                org.apache.hadoop.hive.common.type.Timestamp, AbstractBaseColumn>)
                        val -> {
                            Instant instant = Instant.ofEpochMilli(val.toEpochMilli());
                            LocalDateTime localDateTime =
                                    LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
                            return new TimestampColumn(Timestamp.valueOf(localDateTime));
                        };
            case DATE:
                return (IDeserializationConverter<Date, AbstractBaseColumn>)
                        val -> new SqlDateColumn(val.toEpochDay());
            case BINARY:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>) BytesColumn::new;
            default:
                throw new UnsupportedTypeException(logicalType);
        }
    }

    @Override
    protected ISerializationConverter<List<Object>> createExternalConverter(
            LogicalType logicalType) {

        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return (rowData, index, list) ->
                        list.add(((ColumnRowData) rowData).getField(index).asBoolean());
            case TINYINT:
                return (rowData, index, list) -> list.add(rowData.getByte(index));
            case SMALLINT:
            case INTEGER:
                return (rowData, index, list) ->
                        list.add(((ColumnRowData) rowData).getField(index).asYearInt());
            case BIGINT:
                return (rowData, index, list) ->
                        list.add(((ColumnRowData) rowData).getField(index).asLong());
            case FLOAT:
                return (rowData, index, list) ->
                        list.add(((ColumnRowData) rowData).getField(index).asFloat());
            case DOUBLE:
                return (rowData, index, list) ->
                        list.add(((ColumnRowData) rowData).getField(index).asDouble());
            case DECIMAL:
                return (rowData, index, list) -> {
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
                    list.add(new HiveDecimalWritable(hiveDecimal));
                };
            case VARCHAR:
                return (rowData, index, list) ->
                        list.add(((ColumnRowData) rowData).getField(index).asString());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (rowData, index, list) -> {
                    Timestamp timestamp = ((ColumnRowData) rowData).getField(index).asTimestamp();
                    list.add(
                            org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(
                                    timestamp.getTime(), timestamp.getNanos()));
                };
            case DATE:
                return (rowData, index, list) -> {
                    TimestampData timestamp = rowData.getTimestamp(index, 6);
                    list.add(Date.ofEpochMilli(timestamp.getMillisecond()));
                };
            case BINARY:
                return (rowData, index, list) ->
                        list.add(new BytesWritable(rowData.getBinary(index)));
            default:
                throw new UnsupportedTypeException(logicalType);
        }
    }
}
