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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Date: 2021/04/27 Company: www.dtstack.com
 *
 * @author tudou
 */
public class BytesColumn extends AbstractBaseColumn {
    private String encoding = StandardCharsets.UTF_8.name();

    public BytesColumn(byte[] data) {
        super(data);
    }

    public BytesColumn(byte[] data, String encoding) {
        super(data);
        this.encoding = encoding;
    }

    @Override
    public Boolean asBoolean() {
        if (null == data) {
            return null;
        }
        throw new CastException("Bytes", "Boolean", this.asString());
    }

    @Override
    public byte[] asBytes() {
        if (null == data) {
            return null;
        }
        return (byte[]) data;
    }

    @Override
    public String asString() {
        if (null == data) {
            return null;
        }
        return new String((byte[]) data, Charset.forName(encoding));
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == data) {
            return null;
        }
        throw new CastException("Bytes", "BigDecimal", this.asString());
    }

    @Override
    public Timestamp asTimestamp() {
        if (null == data) {
            return null;
        }
        throw new CastException("Bytes", "Timestamp", this.asString());
    }

    @Override
    public Time asTime() {
        if (null == data) {
            return null;
        }
        throw new CastException("Bytes", "java.sql.Time", this.asString());
    }

    @Override
    public Date asSqlDate() {
        if (null == data) {
            return null;
        }
        throw new CastException("Bytes", "java.sql.Date", this.asString());
    }

    @Override
    public String asTimestampStr() {
        if (null == data) {
            return null;
        }
        throw new CastException("Bytes", "Timestamp", this.asString());
    }
}
