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

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Date: 2021/04/26 Company: www.dtstack.com
 *
 * @author tudou
 */
public class NullColumn extends AbstractBaseColumn {

    public NullColumn() {
        super(null, 0);
    }

    @Override
    public String asString() {
        return null;
    }

    @Override
    public Date asDate() {
        return null;
    }

    @Override
    public byte[] asBytes() {
        return null;
    }

    @Override
    public String type() {
        return "NULL";
    }

    @Override
    public Boolean asBoolean() {
        return null;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return null;
    }

    @Override
    public Double asDouble() {
        return null;
    }

    @Override
    public Timestamp asTimestamp() {
        return null;
    }

    @Override
    public Time asTime() {
        return null;
    }

    @Override
    public java.sql.Date asSqlDate() {
        return null;
    }

    @Override
    public String asTimestampStr() {
        return null;
    }

    @Override
    public Boolean asBooleanInternal() {
        return null;
    }

    @Override
    public byte[] asBytesInternal() {
        return null;
    }

    @Override
    public String asStringInternal() {
        return null;
    }

    @Override
    public BigDecimal asBigDecimalInternal() {
        return null;
    }

    @Override
    public Timestamp asTimestampInternal() {
        return null;
    }

    @Override
    public Time asTimeInternal() {
        return null;
    }

    @Override
    public java.sql.Date asSqlDateInternal() {
        return null;
    }

    @Override
    public String asTimestampStrInternal() {
        return null;
    }
}
