/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.element.column;

import com.dtstack.chunjun.element.AbstractBaseColumn;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class SqlArrayColumn extends AbstractBaseColumn {

    private static final long serialVersionUID = 1L;
    protected Array data;

    public SqlArrayColumn(final Array data) {
        super(data, data.toString().length());
    }

    @Override
    public Boolean asBoolean() {
        return null;
    }

    @Override
    public String type() {
        return "BIGDECIMAL";
    }

    @Override
    public byte[] asBytes() {
        return new byte[0];
    }

    @Override
    public String asString() {
        if (data == null) {
            return null;
        }
        return data.toString();
    }

    @Override
    public BigDecimal asBigDecimal() {
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
    public Date asSqlDate() {
        return null;
    }

    @Override
    public String asTimestampStr() {
        return null;
    }

    @Override
    protected Boolean asBooleanInternal() {
        return null;
    }

    @Override
    protected byte[] asBytesInternal() {
        return new byte[0];
    }

    @Override
    protected String asStringInternal() {
        return null;
    }

    @Override
    protected BigDecimal asBigDecimalInternal() {
        return null;
    }

    @Override
    protected Timestamp asTimestampInternal() {
        return null;
    }

    @Override
    protected Time asTimeInternal() {
        return null;
    }

    @Override
    protected Date asSqlDateInternal() {
        return null;
    }

    @Override
    protected String asTimestampStrInternal() {
        return null;
    }
}
