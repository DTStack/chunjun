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

package com.dtstack.chunjun.connector.rocketmq.converter;

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.rocketmq.common.message.Message;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Objects;

public class RocketMQSqlConverter
        extends AbstractRowConverter<byte[], byte[], Message, LogicalType> {

    private static final long serialVersionUID = 7501509569174874833L;

    private final String encoding;
    private final String[] filedNames;

    public RocketMQSqlConverter(RowType rowType, String encoding, String[] filedNames) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
        this.encoding = encoding;
        this.filedNames = filedNames;
    }

    @Override
    public RowData toInternal(byte[] input) throws Exception {
        String line = new String(input, encoding);
        Map<String, Object> dataMap = JsonUtil.toObject(line, JsonUtil.MAP_TYPE_REFERENCE);
        GenericRowData rowData = new GenericRowData(fieldTypes.length);
        for (int i = 0; i < filedNames.length; i++) {
            if (Objects.nonNull(dataMap.get(filedNames[i]))) {
                Object value = toInternalConverters.get(i).deserialize(dataMap.get(filedNames[i]));
                rowData.setField(i, value);
            } else {
                rowData.setField(i, null);
            }
        }
        return rowData;
    }

    @Override
    public Message toExternal(RowData rowData, Message output) {
        return null;
    }

    @Override
    protected IDeserializationConverter wrapIntoNullableInternalConverter(
            IDeserializationConverter IDeserializationConverter) {
        return super.wrapIntoNullableInternalConverter(IDeserializationConverter);
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> Boolean.valueOf(String.valueOf(val));
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(String.valueOf(val));
            case BINARY:
            case VARBINARY:
                return val -> String.valueOf(val).getBytes();
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                return val -> ((Integer) val).shortValue();
            case INTEGER:
                return val -> Integer.parseInt(String.valueOf(val));
            case BIGINT:
                return val -> Long.parseLong(String.valueOf(val));
            case FLOAT:
                return val -> Float.parseFloat(String.valueOf(val));
            case DOUBLE:
                return val -> Double.parseDouble(String.valueOf(val));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                final int precision = decimalType.getPrecision();
                final int scale = decimalType.getScale();
                return val -> {
                    BigDecimal decimal = new BigDecimal(String.valueOf(val));
                    return DecimalData.fromBigDecimal(decimal, precision, scale);
                };
            case DATE:
                return val ->
                        (int) ((Date.valueOf(String.valueOf(val))).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int)
                                ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp(Timestamp.valueOf(String.valueOf(val)));
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
