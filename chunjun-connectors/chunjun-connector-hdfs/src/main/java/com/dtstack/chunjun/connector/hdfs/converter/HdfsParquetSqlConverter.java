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

import com.dtstack.chunjun.connector.hdfs.util.HdfsUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.List;

public class HdfsParquetSqlConverter
        extends AbstractRowConverter<RowData, RowData, Group, LogicalType> {

    private static final long serialVersionUID = -4819153961537643326L;

    private List<String> columnNameList;

    public HdfsParquetSqlConverter(RowType rowType) {
        super(rowType);
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
    @SuppressWarnings("unchecked")
    public RowData toInternal(RowData input) throws Exception {
        GenericRowData row = new GenericRowData(input.getArity());
        if (input instanceof GenericRowData) {
            GenericRowData genericRowData = (GenericRowData) input;
            for (int i = 0; i < input.getArity(); i++) {
                row.setField(
                        i, toInternalConverters.get(i).deserialize(genericRowData.getField(i)));
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
        for (int index = 0; index < fieldTypes.length; index++) {
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
            ISerializationConverter serializationConverter, LogicalType type) {
        return (rowData, index, group) -> {
            if (rowData == null
                    || rowData.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                // do nothing
            } else {
                serializationConverter.serialize(rowData, index, group);
            }
        };
    }

    @Override
    @SuppressWarnings("all")
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return (IDeserializationConverter<Boolean, Boolean>) val -> val;
            case TINYINT:
                return (IDeserializationConverter<Integer, Byte>) val -> val.byteValue();
            case SMALLINT:
                return (IDeserializationConverter<Integer, Short>) val -> val.shortValue();
            case INTEGER:
                return (IDeserializationConverter<Integer, Integer>) val -> val;
            case BIGINT:
                return (IDeserializationConverter<Long, Long>) val -> val;
            case DATE:
                return (IDeserializationConverter<String, Integer>)
                        val ->
                                (int)
                                        DateUtil.getTimestampFromStr(val)
                                                .toLocalDateTime()
                                                .toLocalDate()
                                                .toEpochDay();
            case FLOAT:
                return (IDeserializationConverter<Float, Float>) val -> val;
            case DOUBLE:
                return (IDeserializationConverter<Double, Double>) val -> val;
            case CHAR:
            case VARCHAR:
                return (IDeserializationConverter<String, StringData>) StringData::fromString;
            case DECIMAL:
                return (IDeserializationConverter<BigDecimal, DecimalData>)
                        val -> DecimalData.fromBigDecimal(val, val.precision(), val.scale());
            case BINARY:
            case VARBINARY:
                return (IDeserializationConverter<byte[], byte[]>) val -> val;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Timestamp, TimestampData>)
                        TimestampData::fromTimestamp;
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    @Override
    protected ISerializationConverter<Group> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (rowData, index, group) -> {};
            case BOOLEAN:
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getBoolean(index));
            case TINYINT:
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getByte(index));
            case SMALLINT:
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getShort(index));
            case INTEGER:
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getInt(index));
            case BIGINT:
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getLong(index));
            case DATE:
                return (rowData, index, group) -> {
                    Date date = Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(index)));
                    group.add(columnNameList.get(index), DateWritable.dateToDays(date));
                };
            case FLOAT:
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getFloat(index));
            case DOUBLE:
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getDouble(index));
            case CHAR:
            case VARCHAR:
                return (rowData, index, group) ->
                        group.add(columnNameList.get(index), rowData.getString(index).toString());
            case DECIMAL:
                return (rowData, index, group) -> {
                    int precision = ((DecimalType) type).getPrecision();
                    int scale = ((DecimalType) type).getScale();
                    HiveDecimal hiveDecimal =
                            HiveDecimal.create(
                                    (rowData.getDecimal(index, precision, scale).toBigDecimal()));
                    hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, precision, scale);
                    group.add(
                            columnNameList.get(index),
                            HdfsUtil.decimalToBinary(hiveDecimal, precision, scale));
                };
            case BINARY:
            case VARBINARY:
                return (rowData, index, group) ->
                        group.add(
                                columnNameList.get(index),
                                Binary.fromReusedByteArray(rowData.getBinary(index)));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (rowData, index, group) -> {
                    TimestampData timestampData =
                            rowData.getTimestamp(index, ((TimestampType) type).getPrecision());
                    group.add(columnNameList.get(index), HdfsUtil.timestampToInt96(timestampData));
                };
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }
}
