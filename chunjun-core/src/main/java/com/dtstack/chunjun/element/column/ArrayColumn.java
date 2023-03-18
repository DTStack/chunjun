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

package com.dtstack.chunjun.element.column;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.throwable.CastException;

import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.ArrayUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

public class ArrayColumn extends AbstractBaseColumn {

    private int size;
    private boolean isPrimitive;

    public ArrayColumn(Object data, int size, boolean isPrimitive) {
        // todo byteSize is not correct
        super(data, 8 * size);
        this.size = size;
        this.isPrimitive = isPrimitive;
    }

    private ArrayColumn(Object data, int size, boolean isPrimitive, int sizeInBytes) {
        // todo byteSize is not correct
        super(data, sizeInBytes);
        this.size = size;
        this.isPrimitive = isPrimitive;
    }

    public ArrayColumn(Object data, LogicalType elementType) {
        this(data, elementType, false);
    }

    public ArrayColumn(Object data, LogicalType elementType, boolean isPrimitive) {
        super(data, 0);
        this.isPrimitive = isPrimitive;
        checkAndSet(elementType);
    }

    public static ArrayColumn from(Object data, int size, boolean isPrimitive) {
        return new ArrayColumn(data, size, isPrimitive, 0);
    }

    private void checkAndSet(LogicalType elementType) {
        Object o = asArray(elementType);
        if (o instanceof int[]) {
            this.size = ((int[]) o).length * 4;
            this.isPrimitive = true;
        } else if (o instanceof long[]) {
            this.size = ((long[]) o).length * 8;
            this.isPrimitive = true;
        } else if (o instanceof double[]) {
            this.size = ((double[]) o).length * 8;
            this.isPrimitive = true;
        } else if (o instanceof float[]) {
            this.size = ((float[]) o).length * 8;
            this.isPrimitive = true;
        } else if (o instanceof short[]) {
            this.size = ((short[]) o).length * 2;
            this.isPrimitive = true;
        } else if (o instanceof byte[]) {
            this.size = ((byte[]) o).length;
            this.isPrimitive = true;
        } else if (o instanceof boolean[]) {
            this.size = ((boolean[]) o).length;
            this.isPrimitive = true;
        } else if (o instanceof Integer[]) {
            this.size = ((Integer[]) o).length * 4;
            this.isPrimitive = false;
        } else if (o instanceof Long[]) {
            this.size = ((Long[]) o).length * 8;
            this.isPrimitive = false;
        } else if (o instanceof Double[]) {
            this.size = ((Double[]) o).length * 8;
            this.isPrimitive = false;
        } else if (o instanceof Float[]) {
            this.size = ((Float[]) o).length * 8;
            this.isPrimitive = false;
        } else if (o instanceof Short[]) {
            this.size = ((Short[]) o).length * 2;
            this.isPrimitive = false;
        } else if (o instanceof Byte[]) {
            this.size = ((Byte[]) o).length;
            this.isPrimitive = false;
        } else if (o instanceof Boolean[]) {
            this.size = ((Boolean[]) o).length;
            this.isPrimitive = false;
        } else if (o instanceof String[]) {
            this.size = ((String[]) o).length * 8;
            this.isPrimitive = false;
        } else if (o instanceof BigDecimal[]) {
            this.size = ((BigDecimal[]) o).length * 12;
            this.isPrimitive = false;
        } else if (o instanceof Date[]) {
            this.size = ((Date[]) o).length * 8;
            this.isPrimitive = false;
        } else if (o instanceof Time[]) {
            this.size = ((Time[]) o).length * 8;
            this.isPrimitive = false;
        } else if (o instanceof Timestamp[]) {
            int precision = getPrecision(elementType);
            if (precision > 3) {
                this.size = ((Timestamp[]) o).length * 12;
            } else {
                this.size = ((Timestamp[]) o).length * 8;
            }
            this.isPrimitive = false;
        } else {
            throw new CastException("Unsupported array type: " + o.getClass());
        }
    }

    @Override
    public String type() {
        return "ARRAY";
    }

    @Override
    public Boolean asBooleanInternal() {
        throw new CastException("ARRAY", "BOOLEAN", this.asStringInternal());
    }

    @Override
    public byte[] asBytesInternal() {
        throw new CastException("ARRAY", "BYTES", this.asStringInternal());
    }

    @Override
    public String asStringInternal() {
        return JSON.toJSONString(this.data);
    }

    @Override
    public BigDecimal asBigDecimalInternal() {
        throw new CastException("ARRAY", "BigDecimal", this.asStringInternal());
    }

    @Override
    public Timestamp asTimestampInternal() {
        throw new CastException("ARRAY", "java.sql.Timestamp", this.asStringInternal());
    }

    @Override
    public Time asTimeInternal() {
        throw new CastException("ARRAY", "java.sql.Time", this.asStringInternal());
    }

    @Override
    public Date asSqlDateInternal() {
        throw new CastException("ARRAY", "java.sql.Date", this.asStringInternal());
    }

    @Override
    public String asTimestampStrInternal() {
        throw new CastException("ARRAY", "java.sql.Timestamp", this.asStringInternal());
    }

    @Override
    public Object asArray() {
        return this.data;
    }

    @Override
    public Object asArray(LogicalType logicalType) {
        if (isPrimitive) {
            switch (logicalType.getTypeRoot()) {
                case BOOLEAN:
                    return asBooleanArray();
                case TINYINT:
                    return asByteArray();
                case SMALLINT:
                    return asShortArray();
                case INTEGER:
                    return asIntArray();
                case BIGINT:
                    return asLongArray();
                case FLOAT:
                    return asFloatArray();
                case DOUBLE:
                    return asDoubleArray();
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported type: " + logicalType.asSummaryString());
            }
        } else {
            switch (logicalType.getTypeRoot()) {
                case BOOLEAN:
                    return asBooleanObjectArray();
                case TINYINT:
                    return asByteObjectArray();
                case SMALLINT:
                    return asShortObjectArray();
                case INTEGER:
                    return asIntObjectArray();
                case BIGINT:
                    return asLongObjectArray();
                case FLOAT:
                    return asFloatObjectArray();
                case DOUBLE:
                    return asDoubleObjectArray();
                case CHAR:
                case VARCHAR:
                    return asStringArray();
                case DECIMAL:
                    return asBigDecimalArray();
                case TIME_WITHOUT_TIME_ZONE:
                    return asTimeArray();
                case DATE:
                    return asSqlDateArray();
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return asTimestampArray();
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported type: " + logicalType.asSummaryString());
            }
        }
    }

    public boolean isPrimitive() {
        return isPrimitive;
    }

    // ------------------------------------------------------------------------------------------
    // Conversion Utilities
    // ------------------------------------------------------------------------------------------

    private boolean anyNull() {
        for (Object element : (Object[]) data) {
            if (element == null) {
                return true;
            }
        }
        return false;
    }

    private void checkNoNull() {
        if (anyNull()) {
            throw new RuntimeException("Primitive array must not contain a null value.");
        }
    }

    public int size() {
        return size;
    }

    public boolean isNullAt(int pos) {
        return !isPrimitive && ((Object[]) data)[pos] == null;
    }

    private Object getObject(int pos) {
        return ((Object[]) data)[pos];
    }

    public Boolean getBoolean(int pos) {
        if (isNullAt(pos)) {
            return null;
        }
        return isPrimitive ? ((boolean[]) data)[pos] : (boolean) getObject(pos);
    }

    public Byte getByte(int pos) {
        if (isNullAt(pos)) {
            return null;
        }
        return isPrimitive ? ((byte[]) data)[pos] : (byte) getObject(pos);
    }

    public Short getShort(int pos) {
        if (isNullAt(pos)) {
            return null;
        }
        return isPrimitive ? ((short[]) data)[pos] : (short) getObject(pos);
    }

    public Integer getInt(int pos) {
        if (isNullAt(pos)) {
            return null;
        }
        return isPrimitive ? ((int[]) data)[pos] : (int) getObject(pos);
    }

    public Long getLong(int pos) {
        if (isNullAt(pos)) {
            return null;
        }
        return isPrimitive ? ((long[]) data)[pos] : (long) getObject(pos);
    }

    public Float getFloat(int pos) {
        if (isNullAt(pos)) {
            return null;
        }
        return isPrimitive ? ((float[]) data)[pos] : (float) getObject(pos);
    }

    public Double getDouble(int pos) {
        if (isNullAt(pos)) {
            return null;
        }
        return isPrimitive ? ((double[]) data)[pos] : (double) getObject(pos);
    }

    public String getString(int pos) {
        return (String) getObject(pos);
    }

    public BigDecimal getDecimal(int pos) {
        return (BigDecimal) getObject(pos);
    }

    public Time getTime(int pos) {
        return (Time) getObject(pos);
    }

    public Date getDate(int pos) {
        return (Date) getObject(pos);
    }

    public Timestamp getTimestamp(int pos) {
        return (Timestamp) getObject(pos);
    }

    public boolean[] asBooleanArray() {
        if (isPrimitive) {
            return (boolean[]) data;
        }
        checkNoNull();
        return ArrayUtils.toPrimitive((Boolean[]) data);
    }

    public byte[] asByteArray() {
        if (isPrimitive) {
            return (byte[]) data;
        }
        checkNoNull();
        return ArrayUtils.toPrimitive((Byte[]) data);
    }

    public short[] asShortArray() {
        if (isPrimitive) {
            return (short[]) data;
        }
        checkNoNull();
        return ArrayUtils.toPrimitive((Short[]) data);
    }

    public int[] asIntArray() {
        if (isPrimitive) {
            return (int[]) data;
        }
        checkNoNull();
        return ArrayUtils.toPrimitive((Integer[]) data);
    }

    public long[] asLongArray() {
        if (isPrimitive) {
            return (long[]) data;
        }
        checkNoNull();
        return ArrayUtils.toPrimitive((Long[]) data);
    }

    public float[] asFloatArray() {
        if (isPrimitive) {
            return (float[]) data;
        }
        checkNoNull();
        return ArrayUtils.toPrimitive((Float[]) data);
    }

    public double[] asDoubleArray() {
        if (isPrimitive) {
            return (double[]) data;
        }
        checkNoNull();
        return ArrayUtils.toPrimitive((Double[]) data);
    }

    private Boolean[] asBooleanObjectArray() {
        Boolean[] booleans = new Boolean[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                booleans[i] = null;
            } else {
                booleans[i] = (Boolean) ((Object[]) data)[i];
            }
        }
        return booleans;
    }

    private Byte[] asByteObjectArray() {
        Byte[] bytes = new Byte[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                bytes[i] = null;
            } else {
                bytes[i] = (Byte) ((Object[]) data)[i];
            }
        }
        return bytes;
    }

    private Short[] asShortObjectArray() {
        Short[] shorts = new Short[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                shorts[i] = null;
            } else {
                shorts[i] = (Short) ((Object[]) data)[i];
            }
        }
        return shorts;
    }

    private Integer[] asIntObjectArray() {
        Integer[] ints = new Integer[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                ints[i] = null;
            } else {
                ints[i] = (Integer) ((Object[]) data)[i];
            }
        }
        return ints;
    }

    private Long[] asLongObjectArray() {
        Long[] longs = new Long[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                longs[i] = null;
            } else {
                longs[i] = (Long) ((Object[]) data)[i];
            }
        }
        return longs;
    }

    private Float[] asFloatObjectArray() {
        Float[] floats = new Float[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                floats[i] = null;
            } else {
                floats[i] = (Float) ((Object[]) data)[i];
            }
        }
        return floats;
    }

    private Double[] asDoubleObjectArray() {
        Double[] doubles = new Double[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                doubles[i] = null;
            } else {
                doubles[i] = (Double) ((Object[]) data)[i];
            }
        }
        return doubles;
    }

    private String[] asStringArray() {
        String[] strings = new String[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                strings[i] = null;
            } else {
                strings[i] = (String) ((Object[]) data)[i];
            }
        }
        return strings;
    }

    private BigDecimal[] asBigDecimalArray() {
        BigDecimal[] bigDecimals = new BigDecimal[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                bigDecimals[i] = null;
            } else {
                bigDecimals[i] = (BigDecimal) ((Object[]) data)[i];
            }
        }
        return bigDecimals;
    }

    private Timestamp[] asTimestampArray() {
        Timestamp[] timestamps = new Timestamp[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                timestamps[i] = null;
            } else {
                timestamps[i] = (Timestamp) ((Object[]) data)[i];
            }
        }
        return timestamps;
    }

    private Time[] asTimeArray() {
        Time[] times = new Time[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                times[i] = null;
            } else {
                times[i] = (Time) ((Object[]) data)[i];
            }
        }
        return times;
    }

    private Date[] asSqlDateArray() {
        Date[] dates = new Date[size];
        for (int i = 0; i < size; i++) {
            if (isNullAt(i)) {
                dates[i] = null;
            } else {
                dates[i] = (Date) ((Object[]) data)[i];
            }
        }
        return dates;
    }

    /**
     * Creates an accessor for getting elements in an internal array data structure at the given
     * position.
     *
     * @param elementType the element type of the array
     */
    public static ElementGetter createElementGetter(LogicalType elementType) {
        final ElementGetter elementGetter;
        // ordered by type root definition
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                elementGetter = ArrayColumn::getString;
                break;
            case BOOLEAN:
                elementGetter = ArrayColumn::getBoolean;
                break;
            case DECIMAL:
                elementGetter = ArrayColumn::getDecimal;
                break;
            case TINYINT:
                elementGetter = ArrayColumn::getByte;
                break;
            case SMALLINT:
                elementGetter = ArrayColumn::getShort;
                break;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                elementGetter = ArrayColumn::getInt;
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                elementGetter = ArrayColumn::getLong;
                break;
            case FLOAT:
                elementGetter = ArrayColumn::getFloat;
                break;
            case DOUBLE:
                elementGetter = ArrayColumn::getDouble;
                break;
            case TIME_WITHOUT_TIME_ZONE:
                elementGetter = ArrayColumn::getTime;
                break;
            case DATE:
                elementGetter = ArrayColumn::getDate;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                elementGetter = ArrayColumn::getTimestamp;
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case BINARY:
            case VARBINARY:
                //                elementGetter =ArrayColumn::getBinary;
                //                break;
            case ARRAY:
                //                elementGetter =ArrayColumn::getArray;
                //                break;
            case MULTISET:
            case MAP:
                //                elementGetter =ArrayColumn::getMap;
                //                break;
            case ROW:
            case STRUCTURED_TYPE:
                //                final int rowFieldCount = getFieldCount(elementType);
                //                elementGetter = (array, pos) -> array.getRow(pos, rowFieldCount);
                //                break;
            case DISTINCT_TYPE:
                //                elementGetter = createElementGetter(((DistinctType)
                // elementType).getSourceType());
                //                break;
            case RAW:
                //                elementGetter =ArrayColumn::getRawValue;
                //                break;
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException();
        }
        if (!elementType.isNullable()) {
            return elementGetter;
        }
        return (array, pos) -> {
            if (array.isNullAt(pos)) {
                return null;
            }
            return elementGetter.getElementOrNull(array, pos);
        };
    }

    /**
     * Accessor for getting the elements of an array during runtime.
     *
     * @see #createElementGetter(LogicalType)
     */
    public interface ElementGetter extends Serializable {
        @Nullable
        Object getElementOrNull(ArrayColumn array, int pos);
    }
}
