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

package com.dtstack.flinkx.connector.phoenix5.converter;

import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedDouble;
import org.apache.phoenix.schema.types.PUnsignedFloat;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PUnsignedSmallint;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PVarchar;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class HBaseRowConverter
        extends AbstractRowConverter<NoTagsKeyValue, Object, NoTagsKeyValue, LogicalType> {

    private static final long serialVersionUID = 3L;

    public transient RowProjector rowProjector;
    // !!! note : PDataType is not serialize，
    private transient List<PDataType> phoenixTypeList;

    public HBaseRowConverter(RowType rowType, RowProjector rowProjector) {
        super(rowType);
        List<String> fieldNames = rowType.getFieldNames();
        phoenixTypeList = new ArrayList<>(fieldNames.size());

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            phoenixTypeList.add(getPDataType(fieldTypes[i].getTypeRoot().toString()));
            this.rowProjector = rowProjector;
        }
    }

    @Override
    protected ISerializationConverter wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, rowData) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                GenericRowData genericRowData = (GenericRowData) rowData;
                genericRowData.setField(index, null);
            } else {
                serializationConverter.serialize(val, index, rowData);
            }
        };
    }

    @Override
    public RowData toInternal(NoTagsKeyValue input) throws Exception {

        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());

        final byte[] bytes = input.getBuffer();
        final int offset = input.getOffset();
        final int length = input.getLength();

        ImmutableBytesWritable pointer = new ImmutableBytesWritable();

        NoTagsKeyValue noTagsKeyValue = new NoTagsKeyValue(bytes, offset, length);
        Result result = Result.create(Collections.singletonList(noTagsKeyValue));
        ResultTuple resultTuple = new ResultTuple(result);

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            ColumnProjector columnProjector = rowProjector.getColumnProjector(i);
            PDataType pDataType = phoenixTypeList.get(i);
            Object value = columnProjector.getValue(resultTuple, pDataType, pointer);
            genericRowData.setField(i, toInternalConverters.get(i).deserialize(value));
        }
        return genericRowData;
    }

    @Override
    public NoTagsKeyValue toExternal(RowData rowData, NoTagsKeyValue output) throws Exception {
        return null;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {

        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case INTEGER:
            case BIGINT:
                return val -> val;
            case TINYINT:
                return val -> (byte) val;
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
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
                return val -> TimestampData.fromTimestamp((Timestamp) val);
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> (byte[]) val;
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedTypeException("Unsupported type:" + type);
        }
    }

    /**
     * 根据字段类型获取Phoenix转换实例 phoenix支持以下数据类型
     *
     * @param type
     * @return
     */
    public PDataType getPDataType(String type) {
        if (StringUtils.isBlank(type)) {
            throw new RuntimeException("type[" + type + "] cannot be blank!");
        }
        switch (type.toUpperCase()) {
            case "INTEGER":
                return PInteger.INSTANCE;
            case "UNSIGNED_INT":
                return PUnsignedInt.INSTANCE;
            case "BIGINT":
                return PLong.INSTANCE;
            case "UNSIGNED_LONG":
                return PUnsignedLong.INSTANCE;
            case "TINYINT":
                return PTinyint.INSTANCE;
            case "UNSIGNED_TINYINT":
                return PUnsignedTinyint.INSTANCE;
            case "SMALLINT":
                return PSmallint.INSTANCE;
            case "UNSIGNED_SMALLINT":
                return PUnsignedSmallint.INSTANCE;
            case "FLOAT":
                return PFloat.INSTANCE;
            case "UNSIGNED_FLOAT":
                return PUnsignedFloat.INSTANCE;
            case "DOUBLE":
                return PDouble.INSTANCE;
            case "UNSIGNED_DOUBLE":
                return PUnsignedDouble.INSTANCE;
            case "DECIMAL":
                return PDecimal.INSTANCE;
            case "BOOLEAN":
                return PBoolean.INSTANCE;
            case "TIME":
                return PTime.INSTANCE;
            case "DATE":
                return PDate.INSTANCE;
            case "TIMESTAMP":
                return PTimestamp.INSTANCE;
            case "UNSIGNED_TIME":
                return PUnsignedTime.INSTANCE;
            case "UNSIGNED_DATE":
                return PUnsignedDate.INSTANCE;
            case "UNSIGNED_TIMESTAMP":
                return PUnsignedTimestamp.INSTANCE;
            case "VARCHAR":
                return PVarchar.INSTANCE;
            case "CHAR":
                return PChar.INSTANCE;
                // 不支持二进制字段类型
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
