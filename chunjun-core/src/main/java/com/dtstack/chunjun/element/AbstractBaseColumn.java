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

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Date: 2021/04/26 Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class AbstractBaseColumn implements Serializable {
    private static final long serialVersionUID = 1L;
    protected Object data;
    protected int byteSize;

    public AbstractBaseColumn(final Object data, int byteSize) {
        this.data = data;
        this.byteSize = byteSize;
    }

    /**
     * Convert data to Boolean type
     *
     * @return
     */
    public abstract Boolean asBoolean();

    /**
     * Convert data to byte[] type
     *
     * @return
     */
    public abstract byte[] asBytes();

    /**
     * Convert data to String type
     *
     * @return
     */
    public abstract String asString();

    /**
     * Convert data to BigDecimal type
     *
     * @return
     */
    public abstract BigDecimal asBigDecimal();

    /**
     * Convert data to Timestamp type
     *
     * @return
     */
    public abstract Timestamp asTimestamp();

    /** Convert data to Time type */
    public abstract Time asTime();

    /** Convert data to java.sql.Date type */
    public abstract java.sql.Date asSqlDate();

    /** Convert data to timestamp string */
    public abstract String asTimestampStr();

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
        if (null == data) {
            return null;
        }
        return this.asInt();
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
