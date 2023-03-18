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

import com.dtstack.chunjun.throwable.CastException;

import java.math.BigDecimal;

public class LongColumn extends NumericColumn {

    public LongColumn(long data) {
        super(data, 8);
    }

    private LongColumn(long data, int byteSize) {
        super(data, byteSize);
    }

    public static LongColumn from(long data) {
        return new LongColumn(data, 8);
    }

    @Override
    public String type() {
        return "LONG";
    }

    @Override
    public byte[] asBytesInternal() {
        throw new CastException("float", "byte[]", this.asStringInternal());
    }

    @Override
    public BigDecimal asBigDecimalInternal() {
        return new BigDecimal((long) data);
    }
}
