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

import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;
import com.dtstack.flinkx.util.DateUtil;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalQueries;
import java.util.List;

/**
 * Date: 2021/06/16
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsTextRowConverter extends AbstractRowConverter<RowData, RowData, List<String>, LogicalType> {

    public HdfsTextRowConverter(RowType rowType) {
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
    public List<String> toExternal(RowData rowData, List<String> list) throws Exception {
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
    protected ISerializationConverter<List<String>> wrapIntoNullableExternalConverter(ISerializationConverter serializationConverter, LogicalType type) {
        return (rowData, index, list) -> {
            if (rowData == null || rowData.isNullAt(index) || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                list.set(index, null);
            } else {
                serializationConverter.serialize(rowData, index, list);
            }
        };
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return (IDeserializationConverter<String, Boolean>) Boolean::getBoolean;
            case TINYINT:
                return (IDeserializationConverter<String, Byte>) Byte::parseByte;
            case SMALLINT:
                return (IDeserializationConverter<String, Short>) Short::parseShort;
            case INTEGER:
                return (IDeserializationConverter<String, Integer>) Integer::parseInt;
            case BIGINT:
                return (IDeserializationConverter<String, Long>) Long::parseLong;
            case DATE:
                return (IDeserializationConverter<String, Integer>) val -> {
                    LocalDate date = DateTimeFormatter.ISO_LOCAL_DATE.parse(val).query(TemporalQueries.localDate());
                    return (int)date.toEpochDay();
                };
            case FLOAT:
                return (IDeserializationConverter<String, Float>) Float::parseFloat;
            case DOUBLE:
                return (IDeserializationConverter<String, Double>) Double::parseDouble;
            case CHAR:
            case VARCHAR:
                return (IDeserializationConverter<String, StringData>) StringData::fromString;
            case DECIMAL:
                return (IDeserializationConverter<String, DecimalData>) val -> {
                    final int precision = ((DecimalType)type).getPrecision();
                    final int scale = ((DecimalType)type).getScale();
                    return DecimalData.fromBigDecimal(new BigDecimal(val), precision, scale);
                };
            case BINARY:
            case VARBINARY:
                return (IDeserializationConverter<String, byte[]>) val -> val.getBytes(StandardCharsets.UTF_8);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<String, TimestampData>) val -> TimestampData.fromTimestamp(new Timestamp(DateUtil.stringToDate(val).getTime()));
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
    protected ISerializationConverter<List<String>> createExternalConverter(LogicalType type) {
        return (rowData, index, list) -> list.set(index, rowData.getString(index).toString());
    }
}
