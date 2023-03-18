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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import static com.dtstack.chunjun.element.ClassSizeUtil.getStringSize;

/**
 * Date: 2021/04/26 Company: www.dtstack.com
 *
 * @author tudou
 */
public class BigDecimalColumn extends AbstractBaseColumn {

    public BigDecimalColumn(BigDecimal data) {
        this(data, data.toString());
    }

    public BigDecimalColumn(long data) {
        this(new BigDecimal(data), String.valueOf(data));
    }

    public BigDecimalColumn(String data) {
        this(new BigDecimal(data), data);
    }

    public BigDecimalColumn(BigInteger data) {
        this(new BigDecimal(data), data.toString());
    }

    private BigDecimalColumn(BigDecimal bigDecimal, String data) {
        super(bigDecimal, getStringSize(data));
    }

    public BigDecimalColumn(BigDecimal data, int byteSize) {
        super(data, byteSize);
    }

    public static BigDecimalColumn from(BigDecimal data) {
        return new BigDecimalColumn(data, 0);
    }

    @Override
    public String asStringInternal() {
        return ((BigDecimal) data).toPlainString();
    }

    @Override
    public Date asDate() {
        if (null == data) {
            return null;
        }
        BigDecimal bigDecimal = (BigDecimal) data;
        return new Date(bigDecimal.longValue());
    }

    @Override
    public byte[] asBytesInternal() {
        throw new CastException("BigDecimal", "Bytes", this.asStringInternal());
    }

    @Override
    public String type() {
        return "BIGDECIMAL";
    }

    @Override
    public Boolean asBooleanInternal() {
        BigDecimal bigDecimal = (BigDecimal) data;
        return bigDecimal.compareTo(BigDecimal.ZERO) != 0;
    }

    @Override
    public BigDecimal asBigDecimalInternal() {
        return (BigDecimal) data;
    }

    @Override
    public Timestamp asTimestampInternal() {
        BigDecimal bigDecimal = (BigDecimal) data;
        return new Timestamp(bigDecimal.longValue());
    }

    @Override
    public java.sql.Date asSqlDateInternal() {
        return java.sql.Date.valueOf(asTimestampInternal().toLocalDateTime().toLocalDate());
    }

    @Override
    public String asTimestampStrInternal() {
        return asTimestampInternal().toString();
    }

    @Override
    public Time asTimeInternal() {
        BigDecimal bigDecimal = (BigDecimal) data;
        return new Time(bigDecimal.longValue());
    }

    @Override
    public Integer asYearInt() {
        return asInt();
    }

    @Override
    public Integer asMonthInt() {
        return asInt() * 12;
    }
}
