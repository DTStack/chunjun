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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Date: 2021/04/26
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class BigDecimalColumn extends AbstractBaseColumn {

    public BigDecimalColumn(BigDecimal data) {
        super(data);
    }

    public BigDecimalColumn(int data) {
        super(new BigDecimal(data));
    }

    public BigDecimalColumn(double data) {
        super(new BigDecimal(data));
    }

    public BigDecimalColumn(float data) {
        super(new BigDecimal(data));
    }

    public BigDecimalColumn(long data) {
        super(new BigDecimal(data));
    }

    @Override
    public int getByteSize(Object data) {
        return null == data ? 0 : data.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public String asString() {
        return data.toString();
    }

    @Override
    public Date asDate() {
        return null;
    }

    @Override
    public byte[] asBytes() {
        return new byte[0];
    }

    @Override
    public Boolean asBoolean() {
        return null;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return (BigDecimal)data;
    }

    @Override
    public Timestamp asTimestamp() {
        return null;
    }
}
