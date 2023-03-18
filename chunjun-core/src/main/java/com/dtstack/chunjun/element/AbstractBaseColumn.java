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
package com.dtstack.chunjun.element;

import com.dtstack.chunjun.element.column.ZonedTimestampColumn;
import com.dtstack.chunjun.throwable.CastException;

import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

/**
 * Date: 2021/04/26 Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class AbstractBaseColumn implements Serializable {
    private static final long serialVersionUID = 1L;
    protected Object data;
    protected int byteSize;
    protected String type;

    public AbstractBaseColumn(final Object data, int byteSize) {
        this.data = data;
        this.byteSize = byteSize;
        this.type = type();
    }

    /**
     * Type of column now.
     *
     * @return
     */
    public abstract String type();

    /**
     * Convert data to Boolean type
     *
     * @return
     */
    public Boolean asBoolean() {
        if (data == null) {
            return null;
        }
        return asBooleanInternal();
    }

    /**
     * Convert data to byte[] type
     *
     * @return
     */
    public byte[] asBytes() {
        if (data == null) {
            return null;
        }
        return asBytesInternal();
    }

    /**
     * Convert data to String type
     *
     * @return
     */
    public String asString() {
        if (data == null) {
            return null;
        }
        return asStringInternal();
    }

    /**
     * Convert data to BigDecimal type
     *
     * @return
     */
    public BigDecimal asBigDecimal() {
        if (data == null) {
            return null;
        }
        return asBigDecimalInternal();
    }

    /**
     * Convert data to Timestamp type
     *
     * @return
     */
    public Timestamp asTimestamp() {
        if (data == null) {
            return null;
        }
        return asTimestampInternal();
    }

    /** Convert data to Time type */
    public Time asTime() {
        if (data == null) {
            return null;
        }
        return asTimeInternal();
    }

    /** Convert data to java.sql.Date type */
    public java.sql.Date asSqlDate() {
        if (data == null) {
            return null;
        }
        return asSqlDateInternal();
    }

    /** Convert data to timestamp string */
    public String asTimestampStr() {
        if (data == null) {
            return null;
        }
        return asTimestampStrInternal();
    }

    /**
     * Convert data to Boolean type
     *
     * @return
     */
    protected abstract Boolean asBooleanInternal();

    /**
     * Convert data to byte[] type
     *
     * @return
     */
    protected abstract byte[] asBytesInternal();

    /**
     * Convert data to String type
     *
     * @return
     */
    protected abstract String asStringInternal();

    /**
     * Convert data to BigDecimal type
     *
     * @return
     */
    protected abstract BigDecimal asBigDecimalInternal();

    /**
     * Convert data to Timestamp type
     *
     * @return
     */
    protected abstract Timestamp asTimestampInternal();

    /** Convert data to Time type */
    protected abstract Time asTimeInternal();

    /** Convert data to java.sql.Date type */
    protected abstract java.sql.Date asSqlDateInternal();

    /** Convert data to timestamp string */
    protected abstract String asTimestampStrInternal();

    /**
     * Convert data to short type
     *
     * @return
     */
    public Byte asByte() {
        if (null == data) {
            return null;
        }
        return this.asBigDecimal().byteValue();
    }

    /**
     * Convert data to short type
     *
     * @return
     */
    public Short asShort() {
        if (null == data) {
            return null;
        }
        return this.asBigDecimal().shortValue();
    }

    /**
     * Convert data to int type
     *
     * @return
     */
    public Integer asInt() {
        if (null == data) {
            return null;
        }
        return this.asBigDecimal().intValue();
    }

    /**
     * Convert data to long type
     *
     * @return
     */
    public Long asLong() {
        if (null == data) {
            return null;
        }
        return this.asBigDecimal().longValue();
    }

    /**
     * Convert data to float type
     *
     * @return
     */
    public Float asFloat() {
        if (null == data) {
            return null;
        }
        return this.asBigDecimal().floatValue();
    }

    /**
     * Convert data to double type
     *
     * @return
     */
    public Double asDouble() {
        if (null == data) {
            return null;
        }
        return this.asBigDecimal().doubleValue();
    }

    /**
     * Convert data to Date type
     *
     * @return
     */
    public Date asDate() {
        if (null == data) {
            return null;
        }
        return new Date(this.asTimestamp().getTime());
    }

    /**
     * Convert data to Year Integer
     *
     * @return
     */
    public Integer asYearInt() {
        if (data == null) {
            return null;
        }
        throw new CastException(this.getClass().getSimpleName(), "Year", this.asString());
    }

    /**
     * Convert data to Year Integer
     *
     * @return
     */
    public Integer asMonthInt() {
        if (data == null) {
            return null;
        }
        throw new CastException(this.getClass().getSimpleName(), "Month", this.asString());
    }

    public Object asArray() {
        throw new CastException(this.getClass().getSimpleName(), "Array", this.asString());
    }

    public Object asArray(LogicalType logicalType) {
        throw new CastException(this.getClass().getSimpleName(), "Array", this.asString());
    }

    public Map<?, ?> asBaseMap() {
        throw new CastException(this.getClass().getSimpleName(), "Map", this.asString());
    }

    public Map<?, ?> asBaseMap(LogicalType keyType, LogicalType valueType) {
        throw new CastException(this.getClass().getSimpleName(), "Map", this.asString());
    }

    public ZonedTimestampColumn asZonedTimestamp() {
        if (null == data) {
            return null;
        }
        return new ZonedTimestampColumn(this.asTimestamp());
    }

    /**
     * Convert data to Binary byte[] type
     *
     * @return
     */
    public byte[] asBinary() {
        return this.asBytes();
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public int getByteSize() {
        return byteSize;
    };

    @Override
    public String toString() {
        return this.data == null ? "" : this.asString();
    }
}
