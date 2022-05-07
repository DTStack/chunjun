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

package com.dtstack.flinkx.element.column;

import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.throwable.CastException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;

/** @author liuliu 2022/1/12 */
public class TimeColumn extends AbstractBaseColumn {

    public TimeColumn(Time data) {
        super(data);
    }

    public TimeColumn(int data) {
        super(Time.valueOf(LocalTime.ofNanoOfDay(data * 1_000_000L)));
    }

    @Override
    public Boolean asBoolean() {
        if (null == data) {
            return null;
        }
        throw new CastException("java.sql.Time", "Boolean", this.asString());
    }

    @Override
    public byte[] asBytes() {
        if (null == data) {
            return null;
        }
        throw new CastException("java.sql.Time", "Bytes", this.asString());
    }

    @Override
    public String asString() {
        if (null == data) {
            return null;
        }
        return data.toString();
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == data) {
            return null;
        }
        return BigDecimal.valueOf(((Time) data).toLocalTime().toNanoOfDay() / 1_000_000L);
    }

    @Override
    public Timestamp asTimestamp() {
        if (null == data) {
            return null;
        }
        return new Timestamp(((Time) data).getTime());
    }

    @Override
    public Time asTime() {
        if (null == data) {
            return null;
        }
        return (Time) data;
    }

    @Override
    public Date asSqlDate() {
        if (null == data) {
            return null;
        }
        throw new CastException("java.sql.Time", "java.sql.Date", this.asString());
    }

    @Override
    public String asTimestampStr() {
        if (null == data) {
            return null;
        }
        return data.toString();
    }

    @Override
    public Integer asInt() {
        throw new CastException("java.sql.Time", "Integer", this.asString());
    }
}
