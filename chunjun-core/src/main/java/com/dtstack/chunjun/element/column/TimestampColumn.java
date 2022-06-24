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
package com.dtstack.chunjun.element.column;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.throwable.CastException;

import org.apache.commons.net.ntp.TimeStamp;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Date: 2021/04/27 Company: www.dtstack.com
 *
 * @author tudou
 */
public class TimestampColumn extends AbstractBaseColumn {

    private static final int DATETIME_STR_LENGTH = 19;

    private final int precision;

    public TimestampColumn(Timestamp data) {
        super(data, 8);
        this.precision = 6;
    }

    public TimestampColumn(long data) {
        super(new Timestamp(data), 8);
        this.precision = 6;
    }

    public TimestampColumn(Date data) {
        super(new Timestamp(data.getTime()), 8);
        this.precision = 6;
    }

    public TimestampColumn(Timestamp data, int precision) {
        super(data, 8);
        this.precision = precision;
    }

    public TimestampColumn(Timestamp data, int precision, int byteSize) {
        super(data, byteSize);
        this.precision = precision;
    }

    public TimestampColumn(long data, int precision) {
        super(new Timestamp(data), 8);
        this.precision = precision;
    }

    public TimestampColumn(long data, int precision, int byteSize) {
        super(new Timestamp(data), byteSize);
        this.precision = precision;
    }

    public TimestampColumn(Date data, int precision) {
        super(new Timestamp(data.getTime()), 8);
        this.precision = precision;
    }

    public static TimestampColumn from(long data, int precision) {
        return new TimestampColumn(data, precision, 0);
    }

    public static TimestampColumn from(Timestamp data, int precision) {
        return new TimestampColumn(data, precision, 0);
    }

    @Override
    public Boolean asBoolean() {
        if (null == data) {
            return null;
        }
        throw new CastException("Timestamp", "Boolean", this.asString());
    }

    @Override
    public byte[] asBytes() {
        if (null == data) {
            return null;
        }
        throw new CastException("Timestamp", "Bytes", this.asString());
    }

    @Override
    public String asString() {
        if (null == data) {
            return null;
        }
        return asTimestampStr();
    }

    /**
     * 根据precision补全/删减0 2022-01-01 00:00:00.0 -> precision=0 -> 2022-01-01 00:00:00 2022-01-01
     * 00:00:00.0 -> precision=3 -> 2022-01-01 00:00:00.000
     *
     * @return 指定precision的Timestamp字符串
     */
    @Override
    public String asTimestampStr() {
        if (null == data) {
            return null;
        }
        // precision>0需要补上'.'的长度
        int resLength =
                (precision == 0 ? DATETIME_STR_LENGTH : DATETIME_STR_LENGTH + 1 + precision);
        String resStr = data.toString();
        if (resStr.length() == resLength) {
            return resStr;
        } else if (resStr.length() > resLength) {
            return resStr.substring(0, resLength);
        } else {
            String fix = String.format("%0" + (resLength - resStr.length()) + "d", 0);
            return resStr + fix;
        }
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == data) {
            return null;
        }
        return new BigDecimal(((TimeStamp) data).getTime());
    }

    @Override
    public Long asLong() {
        if (null == data) {
            return null;
        }
        return ((TimeStamp) data).getTime();
    }

    @Override
    public Short asShort() {
        throw new CastException("java.sql.Timestamp", "Short", this.asString());
    }

    @Override
    public Timestamp asTimestamp() {
        if (null == data) {
            return null;
        }
        return (Timestamp) data;
    }

    @Override
    public Time asTime() {
        if (null == data) {
            return null;
        }
        return new Time(((Timestamp) data).getTime());
    }

    @Override
    public java.sql.Date asSqlDate() {
        if (null == data) {
            return null;
        }
        return java.sql.Date.valueOf(asTimestamp().toLocalDateTime().toLocalDate());
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public Integer asInt() {
        throw new CastException("java.sql.Timestamp", "Integer", this.asString());
    }

    @Override
    public Integer asYearInt() {
        return asTimestamp().toLocalDateTime().getYear();
    }
}
