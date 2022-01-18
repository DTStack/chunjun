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

/**
 * @author tiezhu
 * @since 2021/6/25 星期五
 */
public class ByteColumn extends AbstractBaseColumn {
    public ByteColumn(byte data) {
        super(data);
    }

    public ByteColumn(char data) {
        super(data);
    }

    @Override
    public Boolean asBoolean() {
        return (byte) data != 0x00;
    }

    @Override
    public byte[] asBytes() {
        return new byte[] {(byte) data};
    }

    @Override
    public String asString() {
        return String.valueOf(data);
    }

    @Override
    public BigDecimal asBigDecimal() {
        throw new CastException("Byte", "BigDecimal", String.valueOf(data));
    }

    @Override
    public Timestamp asTimestamp() {
        throw new CastException("Byte", "Timestamp", String.valueOf(data));
    }

    @Override
    public Time asTime() {
        if (null == data) {
            return null;
        }
        throw new CastException("Byte", "java.sql.Time", String.valueOf(data));
    }

    @Override
    public Date asSqlDate() {
        if (null == data) {
            return null;
        }
        throw new CastException("Byte", "java.sql.Date", String.valueOf(data));
    }

    @Override
    public String asTimestampStr() {
        throw new CastException("Byte", "Timestamp", String.valueOf(data));
    }
}
