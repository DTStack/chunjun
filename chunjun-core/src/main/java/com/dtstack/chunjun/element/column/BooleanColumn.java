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
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class BooleanColumn extends AbstractBaseColumn {

    private static final long serialVersionUID = -268960840731455437L;

    public BooleanColumn(boolean data) {
        super(data, 1);
    }

    public BooleanColumn(boolean data, int byteSize) {
        super(data, byteSize);
    }

    public static BooleanColumn from(boolean data) {
        return new BooleanColumn(data, 0);
    }

    @Override
    public Boolean asBoolean() {
        if (null == data) {
            return null;
        }
        return (boolean) data;
    }

    @Override
    public String type() {
        return "BIGDECIMAL";
    }

    @Override
    public byte[] asBytes() {
        if (null == data) {
            return null;
        }
        throw new CastException("boolean", "Bytes", this.asString());
    }

    @Override
    public String asString() {
        if (null == data) {
            return null;
        }
        return (boolean) data ? "true" : "false";
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == data) {
            return null;
        }
        return BigDecimal.valueOf((boolean) data ? 1L : 0L);
    }

    @Override
    public Timestamp asTimestamp() {
        if (null == data) {
            return null;
        }
        throw new CastException("boolean", "Timestamp", this.asString());
    }

    @Override
    public Time asTime() {
        if (null == data) {
            return null;
        }
        throw new CastException("boolean", "java.sql.Time", this.asString());
    }

    @Override
    public Date asSqlDate() {
        if (null == data) {
            return null;
        }
        throw new CastException("boolean", "java.sql.Date", this.asString());
    }

    @Override
    public String asTimestampStr() {
        throw new CastException("boolean", "Timestamp", this.asString());
    }
}
