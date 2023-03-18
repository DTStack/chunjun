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

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

public abstract class NumericColumn extends AbstractBaseColumn {
    public NumericColumn(Number data, int byteSize) {
        super(data, byteSize);
    }

    @Override
    public String asStringInternal() {
        return String.valueOf(data);
    }

    @Override
    public Boolean asBooleanInternal() {
        return ((Number) data).intValue() == 1;
    }

    @Override
    public Byte asByte() {
        return data == null ? null : ((Number) data).byteValue();
    }

    @Override
    public Short asShort() {
        return data == null ? null : ((Number) data).shortValue();
    }

    @Override
    public Integer asInt() {
        return data == null ? null : ((Number) data).intValue();
    }

    @Override
    public Long asLong() {
        return data == null ? null : ((Number) data).longValue();
    }

    @Override
    public Float asFloat() {
        return data == null ? null : ((Number) data).floatValue();
    }

    @Override
    public Double asDouble() {
        return data == null ? null : ((Number) data).doubleValue();
    }

    @Override
    public Date asDate() {
        if (null == data) {
            return null;
        }
        return new Date(asLong());
    }

    @Override
    public Timestamp asTimestampInternal() {
        return new Timestamp(asLong());
    }

    @Override
    public Time asTimeInternal() {
        return new Time(asLong());
    }

    @Override
    public java.sql.Date asSqlDateInternal() {
        return java.sql.Date.valueOf(asTimestampInternal().toLocalDateTime().toLocalDate());
    }

    @Override
    public String asTimestampStrInternal() {
        return asTimestampInternal().toString();
    }
}
