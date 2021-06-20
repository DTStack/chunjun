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

import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.List;

/**
 * Date: 2021/06/16
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsOrcRowConverter extends AbstractRowConverter<RowData, RowData, List<Object>, LogicalType> {

    public HdfsOrcRowConverter(RowType rowType) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters[i] = wrapIntoNullableInternalConverter(createInternalConverter(rowType.getTypeAt(i)));
            toExternalConverters[i] = wrapIntoNullableExternalConverter(createExternalConverter(fieldTypes[i]), fieldTypes[i]);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(RowData input) {
        GenericRowData row = new GenericRowData(input.getArity());
        if(input instanceof GenericRowData){
            GenericRowData genericRowData = (GenericRowData) input;
            for (int i = 0; i < input.getArity(); i++) {
                row.setField(i, toInternalConverters[i].deserialize(genericRowData.getField(i)));
            }
        }else{
            throw new FlinkxRuntimeException("Error RowData type, RowData:[" + input + "] should be instance of GenericRowData.");
        }
        return row;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Object> toExternal(RowData rowData, List<Object> list) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, list);
        }
        return list;
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new FlinkxRuntimeException("HDFS Connector doesn't support Lookup Table Function.");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<GenericRowData> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, rowData) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                rowData.setField(index, null);
            } else {
                serializationConverter.serialize(val, index, rowData);
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
                return (IDeserializationConverter<Byte, Byte>) val -> val;
            case SMALLINT:
                return (IDeserializationConverter<Short, Short>) val -> val;
            case INTEGER:
                return (IDeserializationConverter<Integer, Integer>) val -> val;
            case BIGINT:
                return (IDeserializationConverter<Long, Long>) val -> val;
            case DATE:
                return (IDeserializationConverter<Date, Integer>) val -> (int)val.toLocalDate().toEpochDay();
            case FLOAT:
                return (IDeserializationConverter<Float, Float>) val -> val;
            case DOUBLE:
                return (IDeserializationConverter<Double, Double>) val -> val;
            case CHAR:
            case VARCHAR:
                return (IDeserializationConverter<String, StringData>) StringData::fromString;
            case DECIMAL:
                return (IDeserializationConverter<BigDecimal, DecimalData>) val -> DecimalData.fromBigDecimal(val, val.precision(), val.scale());
            case BINARY:
            case VARBINARY:
                return (IDeserializationConverter<byte[], byte[]>) val -> val;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Timestamp, TimestampData>) TimestampData::fromTimestamp;
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
                throw new UnsupportedTypeException("Unsupported type: " + type);
        }
    }

    @Override
    protected ISerializationConverter<List<Object>> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (rowData, index, list) -> list.set(index, null);
            case BOOLEAN:
                return (rowData, index, list) -> list.set(index, rowData.getBoolean(index));
            case TINYINT:
                return (rowData, index, list) -> list.set(index, rowData.getByte(index));
            case SMALLINT:
                return (rowData, index, list) -> list.set(index, rowData.getShort(index));
            case INTEGER:
                return (rowData, index, list) -> list.set(index, rowData.getInt(index));
            case BIGINT:
                return (rowData, index, list) -> list.set(index, rowData.getLong(index));
            case DATE:
                return (rowData, index, list) -> {
                    list.set(index, Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(index))));
                };
            case FLOAT:
                return (rowData, index, list) -> list.set(index, rowData.getFloat(index));
            case DOUBLE:
                return (rowData, index, list) -> list.set(index, rowData.getDouble(index));
            case CHAR:
            case VARCHAR:
                return (rowData, index, list) -> list.set(index, rowData.getString(index).toString());
            case DECIMAL:
                return (rowData, index, list) -> {
                    HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(rowData.getString(index).toString()));
                    hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, ((DecimalType)type).getPrecision(), ((DecimalType)type).getScale());
                    list.set(index, new HiveDecimalWritable(hiveDecimal));
                };
            case BINARY:
            case VARBINARY:
                return (rowData, index, list) -> list.set(index, rowData.getBinary(index));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (rowData, index, list) -> list.set(index, rowData.getTimestamp(index, ((TimestampType)type).getPrecision()).toTimestamp());
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
                throw new UnsupportedTypeException("Unsupported type: " + type);
        }
    }
}
