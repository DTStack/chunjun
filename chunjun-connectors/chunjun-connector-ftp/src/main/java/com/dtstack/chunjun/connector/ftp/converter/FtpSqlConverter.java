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

package com.dtstack.chunjun.connector.ftp.converter;

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;

public class FtpSqlConverter extends AbstractRowConverter<String[], String, String, LogicalType> {

    private static final long serialVersionUID = 4127516611259169686L;

    private DeserializationSchema<RowData> valueDeserialization;

    private SerializationSchema<RowData> valueSerialization;

    public FtpSqlConverter(DeserializationSchema<RowData> valueDeserialization) {
        this.valueDeserialization = valueDeserialization;
    }

    public FtpSqlConverter(SerializationSchema<RowData> valueSerialization) {
        this.valueSerialization = valueSerialization;
    }

    public FtpSqlConverter(RowType rowType) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
        }
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case INTEGER:
                return val -> Integer.valueOf((String) val);
            case BIGINT:
                return val -> Long.valueOf((String) val);
            case FLOAT:
                return val -> Float.valueOf((String) val);
            case DOUBLE:
                return val -> Double.valueOf((String) val);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                final int precision = decimalType.getPrecision();
                final int scale = decimalType.getScale();
                return val -> {
                    BigDecimal decimal = new BigDecimal(String.valueOf(val));
                    return DecimalData.fromBigDecimal(decimal, precision, scale);
                };
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString((String) val);
            case DATE:
                return val ->
                        (int) ((Date.valueOf(String.valueOf(val))).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int)
                                ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            default:
                throw new UnsupportedOperationException(type.toString());
        }
    }

    @Override
    public RowData toInternal(String[] input) throws Exception {
        GenericRowData rowData = new GenericRowData(input.length);
        for (int i = 0; i < fieldTypes.length; i++) {
            rowData.setField(i, toInternalConverters.get(i).deserialize(input[i]));
        }
        return rowData;
    }

    @Override
    public String toExternal(RowData rowData, String output) throws Exception {
        valueSerialization.open(new DummyInitializationContext());
        byte[] serialize = valueSerialization.serialize(rowData);
        return new String(serialize);
    }
}
