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

/**
 * Date: 2021/04/26 Company: www.dtstack.com
 *
 * @author tudou
 */
public class BigDecimalColumn extends AbstractBaseColumn {

    public BigDecimalColumn(BigDecimal data) {
        super(data);
    }

    public BigDecimalColumn(int data) {
        super(new BigDecimal(data));
    }

    public BigDecimalColumn(double data) {
        super(new BigDecimal(String.valueOf(data)));
    }

    public BigDecimalColumn(float data) {
        super(new BigDecimal(String.valueOf(data)));
    }

    public BigDecimalColumn(long data) {
        super(new BigDecimal(data));
    }

    public BigDecimalColumn(String data) {
        super(new BigDecimal(data));
    }

    public BigDecimalColumn(BigInteger data) {
        super(new BigDecimal(data));
    }

    public BigDecimalColumn(short data) {
        super(new BigDecimal(data));
    }

    @Override
    public String asString() {
        if (null == data) {
            return null;
        }

        return data.toString();
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
    public byte[] asBytes() {
        if (null == data) {
            return null;
        }
        throw new CastException("BigDecimal", "Bytes", this.asString());
    }

    @Override
    public Boolean asBoolean() {
        if (null == data) {
            return null;
        }
        BigDecimal bigDecimal = (BigDecimal) data;
        return bigDecimal.compareTo(BigDecimal.ZERO) != 0;
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == data) {
            return null;
        }
        return (BigDecimal) data;
    }

    @Override
    public Timestamp asTimestamp() {
        if (null == data) {
            return null;
        }
        BigDecimal bigDecimal = (BigDecimal) data;
        return new Timestamp(bigDecimal.longValue());
    }

    @Override
    public java.sql.Date asSqlDate() {
        if (null == data) {
            return null;
        }
        return java.sql.Date.valueOf(asTimestamp().toLocalDateTime().toLocalDate());
    }

    @Override
    public String asTimestampStr() {
        return asTimestamp().toString();
    }

    @Override
    public Time asTime() {
        if (null == data) {
            return null;
        }
        BigDecimal bigDecimal = (BigDecimal) data;
        return new Time(bigDecimal.longValue());
    }
}
