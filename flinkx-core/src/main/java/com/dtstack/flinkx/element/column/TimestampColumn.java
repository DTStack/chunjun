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
package com.dtstack.flinkx.element.column;

import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.throwable.CastException;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Date: 2021/04/27 Company: www.dtstack.com
 *
 * @author tudou
 */
public class TimestampColumn extends AbstractBaseColumn {

    public TimestampColumn(Timestamp data) {
        super(data);
    }

    public TimestampColumn(long data) {
        super(new Timestamp(data));
    }

    public TimestampColumn(Date data) {
        super(new Timestamp(data.getTime()));
    }

    @Override
    public Boolean asBoolean() {
        if (null == data) {
            return null;
        }
        throw new CastException("Timestamp", "Boolean", this.asString());
    }

    @Override
    public byte[] asBytes() {
        if (null == data) {
            return null;
        }
        throw new CastException("Timestamp", "Bytes", this.asString());
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
        throw new CastException("Timestamp", "BigDecimal", this.asString());
    }

    @Override
    public Timestamp asTimestamp() {
        if (null == data) {
            return null;
        }
        return (Timestamp) data;
    }
}
