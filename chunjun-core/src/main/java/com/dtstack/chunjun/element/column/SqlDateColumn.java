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
import com.dtstack.chunjun.throwable.CastException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;

/** @author liuliu 2022/1/12 */
public class SqlDateColumn extends AbstractBaseColumn {
    public SqlDateColumn(Date data) {
        super(data, 8);
    }

    public SqlDateColumn(Date data, int byteSize) {
        super(data, 0);
    }

    public SqlDateColumn(long data) {
        super(Date.valueOf(LocalDate.ofEpochDay(data)), 8);
    }

    public static SqlDateColumn from(Date date) {
        return new SqlDateColumn(date, 0);
    }

    @Override
    public Boolean asBoolean() {
        if (null == data) {
            return null;
        }
        throw new CastException("java.sql.Date", "Boolean", this.asString());
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
        throw new CastException("java.sql.Date", "Bytes", this.asString());
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
        return BigDecimal.valueOf(((Date) data).toLocalDate().toEpochDay());
    }

    @Override
    public Timestamp asTimestamp() {
        if (null == data) {
            return null;
        }
        return new Timestamp(((Date) data).getTime());
    }

    @Override
    public Time asTime() {
        if (null == data) {
            return null;
        }
        throw new CastException("java.sql.Date", "java.sql.Time", this.asString());
    }

    @Override
    public Date asSqlDate() {
        if (null == data) {
            return null;
        }
        return (Date) data;
    }

    @Override
    public String asTimestampStr() {
        if (null == data) {
            return null;
        }
        return data.toString();
    }

    @Override
    public Integer asYearInt() {
        return asTimestamp().toLocalDateTime().getYear();
    }
}
