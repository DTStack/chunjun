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
        super(data, data.length);
    }

    public BytesColumn(byte[] data, String encoding) {
        super(data, data.length);
        this.encoding = encoding;
    }

    private BytesColumn(byte[] data, int byteSize) {
        super(data, byteSize);
    }

    public BytesColumn(byte[] data, int byteSize, String encoding) {
        super(data, byteSize);
        this.encoding = encoding;
    }

    public BytesColumn(Object[] data, int byteSize, String encoding) {
        super(data, byteSize);
        this.encoding = encoding;
    }

    public static BytesColumn from(byte[] data) {
        return new BytesColumn(data, 0);
    }

    @Override
    public String type() {
        return "BYTES";
    }

    @Override
    public Boolean asBooleanInternal() {
        throw new CastException("Bytes", "Boolean", this.asString());
    }

    @Override
    public byte[] asBytesInternal() {
        return (byte[]) data;
    }

    @Override
    public String asStringInternal() {
        return new String((byte[]) data, Charset.forName(encoding));
    }

    @Override
    public BigDecimal asBigDecimalInternal() {
        throw new CastException("Bytes", "BigDecimal", this.asStringInternal());
    }

    @Override
    public Timestamp asTimestampInternal() {
        throw new CastException("Bytes", "Timestamp", this.asStringInternal());
    }

    @Override
    public Time asTimeInternal() {
        throw new CastException("Bytes", "java.sql.Time", this.asStringInternal());
    }

    @Override
    public Date asSqlDateInternal() {
        throw new CastException("Bytes", "java.sql.Date", this.asStringInternal());
    }

    @Override
    public String asTimestampStrInternal() {
        throw new CastException("Bytes", "Timestamp", this.asStringInternal());
    }
}
